from typing import Dict, List, Union, Optional
import time
import uuid
import etcd3
from abc import ABC, abstractmethod
from ._api_ext_chan.mpsc import (
    MPSCChanProducer, 
    MPSCChanConsumer,
    ChanType,
    ChanRole,
    _new_etcd_meta_key,
)
from ._api_ext_chan.mpmc import (
    MPMCChanProducer,
    MPMCChanConsumer,
    _new_mpmc_meta_key,
    _new_mpmc_next_channel_id_key,
)
from .kvclient.kvclient_interface import KvClient
from .api_error import Result, ApiError
from .api_error import (
    Result,
    InvalidConfigurationError,
    PayloadLeaseNotFoundError,
    ChanKeyNotFoundError,
    ChanCreateError,
    ChanBindError,
    ChanUnBindError,
    EtcdError,
    InternalError,
    TransportName,
    TransportUser,
)

import logging

MQ_UNIQUE_KEY_PREFIX = "/mq_unique_keys/"
MQ_UNIQUE_LOCK_PREFIX = "/mq_unique_locks/"
MQ_UNIQUE_LOCK_WAIT_MULTIPLIER = 200
MQ_UNIQUE_FAST_BIND_READ_RETRY_ATTEMPTS = 3
MQ_UNIQUE_FAST_BIND_READ_RETRY_BASE_SECONDS = 0.2
MQ_UNIQUE_FAST_BIND_READ_RETRY_MAX_SECONDS = 1.0


def _new_unique_mapping_key(unique_id: str) -> str:
    if not isinstance(unique_id, str):
        raise ValueError(f"unique_id must be str, got {type(unique_id).__name__}")
    if unique_id == "":
        raise ValueError("unique_id must not be empty")
    return f"{MQ_UNIQUE_KEY_PREFIX}{unique_id}"


def _new_unique_lock_key(unique_id: str) -> str:
    if not isinstance(unique_id, str):
        raise ValueError(f"unique_id must be str, got {type(unique_id).__name__}")
    if unique_id == "":
        raise ValueError("unique_id must not be empty")
    return f"{MQ_UNIQUE_LOCK_PREFIX}{unique_id}"


def new_or_bind_with_unique_key(
    api: KvClient,
    chan_config: Dict[str, int],
    unique_id: str,
    chan_type: ChanType,
    chan_role: ChanRole,
) -> Result[Union[MPSCChanProducer, MPSCChanConsumer, MPMCChanProducer, MPMCChanConsumer], ApiError]:
    """
    Use an etcd distributed lock to bind or create a channel.

    If chan_id mapped by unique_id exists, bind to that channel; otherwise create a
    new channel and publish the mapping.
    
    Args:
        api(KvClient): KV store API
        chan_config(Dict[str, int]): channel config
        unique_id(str): unique identifier
        chan_type(ChanType): channel type
        chan_role(ChanRole): channel role
        etcd_client(etcd3.Etcd3Client): etcd client
        
    Returns:
        Result[Union[MPSCChanProducer, MPSCChanConsumer, MPMCChanProducer, MPMCChanConsumer], ApiError]:
            On success, returns producer/consumer object; on failure, returns an error.
    """
    lock_key = _new_unique_lock_key(unique_id)
    unique_key = _new_unique_mapping_key(unique_id)
    etcd_client = new_etcd_client(api)
    if not etcd_client.is_ok():
        return Result[Union[MPSCChanProducer, MPSCChanConsumer, MPMCChanProducer, MPMCChanConsumer], ApiError].new_error(
            EtcdError(message=f"Failed to create etcd client: {etcd_client.unwrap_error()}")
        )
    etcd_client = etcd_client.unwrap()
    assert etcd_client is not None, "etcd_client should not be None"

    def _is_lease_not_found_error(err: Exception) -> bool:
        msg = str(err).lower()
        return "lease not found" in msg or "requested lease not found" in msg

    def _revoke_lock_lease_if_present(*, lease_id: int) -> None:
        try:
            etcd_client.revoke_lease(int(lease_id))
        except Exception as e:  # noqa: BLE001
            if _is_lease_not_found_error(e):
                return
            logging.warning("failed to revoke unused etcd lock lease: lease_id=%s err=%s", lease_id, e)

    def _acquire_lock(*, key: str, ttl_seconds: int) -> str:
        """Acquire a simple distributed mutex without etcd watch APIs.

        etcd3.Lock uses watch/streaming internally. In our CI, grpc watch can
        return a rendezvous error object which breaks `watch_once()` call sites.
        To keep the behavior simple and deterministic, we implement a polling
        lock using a compare-and-put transaction with a lease.
        """
        if ttl_seconds <= 0:
            raise ValueError(f"invalid ttl_seconds: {ttl_seconds}")

        token = uuid.uuid4().hex
        # Bound the wait by a multiple of the TTL. If we cannot acquire the lock
        # within this window, it likely indicates a stale lock key (or broken
        # lease management) and the test should fail loudly.
        lock_wait_seconds = float(ttl_seconds) * MQ_UNIQUE_LOCK_WAIT_MULTIPLIER
        deadline = time.time() + lock_wait_seconds
        last_lease_error: Optional[Exception] = None
        while True:
            current_value, _ = etcd_client.get(key)
            if current_value is not None:
                if time.time() >= deadline:
                    msg = (
                        f"Timed out acquiring etcd lock key={key!r} after {lock_wait_seconds}s; "
                        "this is treated as a missing prerequisite (stale lock) rather than a retryable condition."
                    )
                    if last_lease_error is not None:
                        raise RuntimeError(f"{msg} last_lease_error={last_lease_error}") from last_lease_error
                    raise RuntimeError(msg)
                time.sleep(0.2)
                continue

            lease = etcd_client.lease(ttl_seconds)
            lease_id = int(lease.id)
            should_revoke_lease = True
            success = False
            try:
                success, _ = etcd_client.transaction(
                    compare=[etcd_client.transactions.create(key) == 0],
                    success=[etcd_client.transactions.put(key, token.encode("utf-8"), lease=lease)],
                    failure=[],
                )
                if success:
                    should_revoke_lease = False
            except Exception as e:  # noqa: BLE001
                if not _is_lease_not_found_error(e):
                    raise
                last_lease_error = e
            finally:
                if should_revoke_lease:
                    _revoke_lock_lease_if_present(lease_id=lease_id)
            if success:
                return token
            if time.time() >= deadline:
                raise RuntimeError(
                    f"Timed out acquiring etcd lock key={key!r} after {lock_wait_seconds}s; "
                    "this is treated as a missing prerequisite (stale lock) rather than a retryable condition."
                )
            time.sleep(0.2)

    def _release_lock(*, key: str, token: str) -> None:
        # Delete only if we still own it. If this fails, do not ignore it:
        # leaving stale locks behind makes future runs non-deterministic.
        success, _ = etcd_client.transaction(
            compare=[etcd_client.transactions.value(key) == token.encode("utf-8")],
            success=[etcd_client.transactions.delete(key)],
            failure=[],
        )
        if not success:
            raise RuntimeError(f"Failed to release etcd lock key={key!r}; token mismatch or key missing")

    def _meta_key_for_chan(chan_type: ChanType, chan_id: str) -> str:
        if chan_type == ChanType.MPSC:
            return _new_etcd_meta_key(chan_id)
        if chan_type == ChanType.MPMC:
            return _new_mpmc_meta_key(chan_id)
        raise InvalidConfigurationError(message=f"Invalid channel type: {chan_type}")

    def _read_unique_chan_id() -> Result[tuple[bool, Optional[str]], ApiError]:
        value, _ = etcd_client.get(unique_key)
        if value is None:
            return Result[tuple[bool, Optional[str]], ApiError].new_ok((False, None))
        try:
            chan_id = value.decode()
            if not chan_id.isdigit():
                raise ValueError(f"chan_id is not a digit-only string: {chan_id!r}")
        except (ValueError, UnicodeDecodeError) as e:
            # Do not delete the key here. Deleting the unique mapping during a running test
            # can split producers/consumers across different channel ids (split-brain).
            return Result[tuple[bool, Optional[str]], ApiError].new_error(
                EtcdError(
                    message=(
                        f"Invalid chan_id stored in etcd key {unique_key!r}: {value!r}, err={e}. "
                        "Please delete the key manually before retrying."
                    )
                )
            )
        return Result[tuple[bool, Optional[str]], ApiError].new_ok((True, chan_id))

    def _is_stale_bind_error(err: ApiError) -> bool:
        if (
            chan_type == ChanType.MPMC
            and isinstance(err, InvalidConfigurationError)
            and getattr(err, "config_key", None) == "mpmc_meta_stale"
        ):
            return True
        if chan_type == ChanType.MPSC:
            return (
                isinstance(err, PayloadLeaseNotFoundError)
                or (
                    isinstance(err, InvalidConfigurationError)
                    and "register_lease(payload kvclient)" in str(err)
                    and ("LeaseNotFound" in str(err) or "lease not found" in str(err))
                )
            )
        return False

    def _cleanup_stale_mapping_under_lock(*, expected_chan_id: str) -> Result[bool, ApiError]:
        current_chan_id_res = _read_unique_chan_id()
        if not current_chan_id_res.is_ok():
            return Result[bool, ApiError].new_error(current_chan_id_res.unwrap_error())
        current_chan_id_exists, current_chan_id = current_chan_id_res.unwrap()
        if not current_chan_id_exists:
            logging.warning(
                "stale bind cleanup sees mapping already absent; retry resolve. unique_key=%s expected_chan_id=%s",
                unique_key,
                expected_chan_id,
            )
            return Result[bool, ApiError].new_ok(True)
        if current_chan_id != expected_chan_id:
            logging.warning(
                "stale bind cleanup sees mapping already remapped; retry resolve. unique_key=%s expected_chan_id=%s current_chan_id=%s",
                unique_key,
                expected_chan_id,
                current_chan_id,
            )
            return Result[bool, ApiError].new_ok(True)

        if chan_type == ChanType.MPMC:
            meta_key = _new_mpmc_meta_key(expected_chan_id)
            next_id_key = _new_mpmc_next_channel_id_key(expected_chan_id)
            try:
                etcd_client.delete(unique_key)
                etcd_client.delete(meta_key)
                etcd_client.delete(next_id_key)
            except Exception as e:  # noqa: BLE001
                return Result[bool, ApiError].new_error(
                    EtcdError(
                        message=(
                            "Failed to delete stale MPMC mapping/meta under lock. "
                            f"unique_key={unique_key!r} chan_id={expected_chan_id} err={e}"
                        )
                    )
                )
            return Result[bool, ApiError].new_ok(True)

        if chan_type == ChanType.MPSC:
            meta_key = _new_etcd_meta_key(expected_chan_id)
            chan_prefix = f"/channels/{expected_chan_id}/"
            try:
                etcd_client.delete(unique_key)
                etcd_client.delete(meta_key)
                etcd_client.delete_prefix(chan_prefix)
            except Exception as e:  # noqa: BLE001
                return Result[bool, ApiError].new_error(
                    EtcdError(
                        message=(
                            "Failed to delete stale MPSC mapping/meta under lock. "
                            f"unique_key={unique_key!r} chan_id={expected_chan_id} err={e}"
                        )
                    )
                )
            return Result[bool, ApiError].new_ok(True)

        return Result[bool, ApiError].new_error(
            InvalidConfigurationError(message=f"Invalid channel type for stale cleanup: {chan_type}")
        )

    def _bind_existing_channel(
        existing_chan_id: str,
    ) -> Result[Union[MPSCChanProducer, MPSCChanConsumer, MPMCChanProducer, MPMCChanConsumer], ApiError]:
        logging.debug(
            "new_or_bind_with_unique_key %s bind to existing channel %s outside lock",
            unique_id,
            existing_chan_id,
        )
        result = _construct_bound_channel(
            api,
            chan_config,
            existing_chan_id,
            chan_type,
            chan_role,
            etcd_client,
        )
        if not result.is_ok():
            err = result.unwrap_error()
            if _is_stale_bind_error(err):
                return Result[Union[MPSCChanProducer, MPSCChanConsumer, MPMCChanProducer, MPMCChanConsumer], ApiError].new_error(err)
            return Result[Union[MPSCChanProducer, MPSCChanConsumer, MPMCChanProducer, MPMCChanConsumer], ApiError].new_error(
                ChanBindError(f"Bind failed for chan_id={existing_chan_id}: {err}")
            )

        return Result[
            Union[MPSCChanProducer, MPSCChanConsumer, MPMCChanProducer, MPMCChanConsumer],
            ApiError,
        ].new_ok(result.unwrap())

    def _try_bind_existing_channel_without_lock() -> Result[object, ApiError]:
        """
        Bind fast when the unique mapping is already published.

        The distributed lock is only needed to create a channel or clean stale
        metadata. Existing mappings can bind directly and avoid turning a large
        producer fan-out into hundreds of serialized etcd lock acquisitions.
        """
        last_read_exc: Optional[Exception] = None
        for read_attempt in range(MQ_UNIQUE_FAST_BIND_READ_RETRY_ATTEMPTS):
            try:
                existing_chan_id_res = _read_unique_chan_id()
                break
            except Exception as e:  # noqa: BLE001
                last_read_exc = e
                if read_attempt >= MQ_UNIQUE_FAST_BIND_READ_RETRY_ATTEMPTS - 1:
                    return Result[object, ApiError].new_error(
                        EtcdError(
                            message=f"Failed to read unique key before lock: {unique_key!r}, err={e}",
                            component="api_ext_chan.new_or_bind_with_unique_key",
                            transport=TransportName.GRPC,
                            transport_user=TransportUser.ETCD,
                        )
                    )
                sleep_s = min(
                    MQ_UNIQUE_FAST_BIND_READ_RETRY_MAX_SECONDS,
                    MQ_UNIQUE_FAST_BIND_READ_RETRY_BASE_SECONDS * (2 ** read_attempt),
                )
                logging.warning(
                    "unique key fast-bind read failed transiently; retrying: unique_key=%s attempt=%s/%s sleep_seconds=%.2f err=%s",
                    unique_key,
                    read_attempt + 1,
                    MQ_UNIQUE_FAST_BIND_READ_RETRY_ATTEMPTS,
                    sleep_s,
                    e,
                )
                time.sleep(sleep_s)
        else:
            assert last_read_exc is not None
            return Result[object, ApiError].new_error(
                EtcdError(
                    message=f"Failed to read unique key before lock: {unique_key!r}, err={last_read_exc}",
                    component="api_ext_chan.new_or_bind_with_unique_key",
                    transport=TransportName.GRPC,
                    transport_user=TransportUser.ETCD,
                )
            )

        if not existing_chan_id_res.is_ok():
            return Result[object, ApiError].new_error(existing_chan_id_res.unwrap_error())

        existing_chan_id_exists, existing_chan_id = existing_chan_id_res.unwrap()
        if not existing_chan_id_exists:
            return Result[object, ApiError].new_ok(("no_mapping", None))

        assert existing_chan_id is not None
        bind_res = _bind_existing_channel(existing_chan_id)
        if bind_res.is_ok():
            return Result[object, ApiError].new_ok(bind_res.unwrap())
        return Result[object, ApiError].new_error(bind_res.unwrap_error())

    def _resolve_or_create_under_lock() -> Result[object, ApiError]:
        """
        Resolve the unique key under lock.

        Returns:
        - ("bind_existing", chan_id): mapping is already published; bind should happen outside the lock.
        - producer/consumer object: new channel was created and published under the lock.
        - error: retry or failure.
        """
        existing_chan_id_res = _read_unique_chan_id()
        if not existing_chan_id_res.is_ok():
            return Result[object, ApiError].new_error(existing_chan_id_res.unwrap_error())
        existing_chan_id_exists, existing_chan_id = existing_chan_id_res.unwrap()

        if existing_chan_id_exists:
            meta_key = _meta_key_for_chan(chan_type, existing_chan_id)
            try:
                meta_val, _meta = etcd_client.get(meta_key)
            except Exception as e:
                return Result[object, ApiError].new_error(
                    EtcdError(
                        message=(
                            f"Failed to read channel meta key={meta_key} for existing chan_id={existing_chan_id} "
                            f"(unique_key={unique_key!r}): {e}"
                        )
                    )
                )
            if meta_val is None:
                # Stale unique mapping: unique_key points to a chan_id whose meta is missing.
                # Under the distributed lock, it is safe to remove the stale mapping and create a new channel.
                try:
                    etcd_client.delete(unique_key)
                except Exception as e:
                    return Result[object, ApiError].new_error(
                        EtcdError(
                            message=(
                                f"Failed to delete stale unique mapping {unique_key!r} (chan_id={existing_chan_id}): {e}"
                            )
                        )
                    )
                logging.warning(
                    "Deleted stale unique mapping; will recreate channel under lock. unique_key=%s chan_id=%s meta_key=%s",
                    unique_key,
                    existing_chan_id,
                    meta_key,
                )
                existing_chan_id = None

        if existing_chan_id is not None:
            return Result[object, ApiError].new_ok(("bind_existing", existing_chan_id))

        logging.debug(f"new_or_bind_with_unique_key {unique_id} create new channel")
        result = chan_new(api, chan_config, chan_type, chan_role, etcd_client)
        if not result.is_ok():
            return Result[object, ApiError].new_error(
                ChanCreateError(f"Failed to create new channel: {result.unwrap_error()}")
            )

        new_chan_id = result.unwrap()
        if not isinstance(new_chan_id, str) or not new_chan_id.isdigit():
            return Result[object, ApiError].new_error(
                ChanCreateError(f"Failed to create new channel: invalid chan_id {new_chan_id!r}")
            )
        success, _ = etcd_client.transaction(
            compare=[etcd_client.transactions.create(unique_key) == 0],
            success=[etcd_client.transactions.put(unique_key, new_chan_id.encode())],
            failure=[],
        )
        if not success:
            created = get_chan_by_id(chan_type, new_chan_id)
            if not created.is_ok():
                return Result[object, ApiError].new_error(
                    InternalError(
                        message=(
                            f"unique key publish failed and created chan_id={new_chan_id} was not found "
                            f"in registry for cleanup: {created.unwrap_error()}"
                        )
                    )
                )
            close_res = created.unwrap().close()
            if not close_res.is_ok():
                return Result[object, ApiError].new_error(
                    InternalError(
                        message=(
                            f"unique key publish failed and cleanup close() failed for chan_id={new_chan_id}: "
                            f"{close_res.unwrap_error()}"
                        )
                    )
                )
            close_res.unwrap()
            del_chan_by_id(chan_type, new_chan_id)
            return Result[object, ApiError].new_error(
                ChanBindError(
                    f"RETRY: unique key {unique_key} already exists; created chan_id={new_chan_id} was dropped"
                )
            )

        result = get_chan_by_id(chan_type, new_chan_id)
        del_chan_by_id(chan_type, new_chan_id)
        if result.is_ok():
            return Result[object, ApiError].new_ok(result.unwrap())
        return Result[object, ApiError].new_error(
            ChanCreateError(f"New channel {new_chan_id} not found in active nodes, error: {result.unwrap_error()}")
        )

    def _call_resolve_under_lock() -> Result[object, ApiError]:
        try:
            return _resolve_or_create_under_lock()
        except Exception as e:  # noqa: BLE001
            # Keep retry logic in one place: exceptions should not bypass the
            # "retry once under lock" policy.
            logging.warning(
                "new_or_bind_with_unique_key caught exception under lock: unique_key=%s err=%s",
                unique_key,
                e,
            )
            return Result[object, ApiError].new_error(
                ChanCreateError(f"RETRY: exception in new_or_bind_with_unique_key: {e}")
            )

    final_value: Optional[object] = None
    final_error: Optional[ApiError] = None
    max_attempts = 3
    for attempt_idx in range(max_attempts):
        fast_bind_res = _try_bind_existing_channel_without_lock()
        if fast_bind_res.is_ok():
            fast_bind_value = fast_bind_res.unwrap()
            if not (
                isinstance(fast_bind_value, tuple)
                and len(fast_bind_value) == 2
                and fast_bind_value[0] == "no_mapping"
            ):
                return Result[Union[MPSCChanProducer, MPSCChanConsumer, MPMCChanProducer, MPMCChanConsumer], ApiError].new_ok(fast_bind_value)
        else:
            fast_bind_err = fast_bind_res.unwrap_error()
            if not _is_stale_bind_error(fast_bind_err):
                return Result[Union[MPSCChanProducer, MPSCChanConsumer, MPMCChanProducer, MPMCChanConsumer], ApiError].new_error(fast_bind_err)

            logging.warning(
                "existing channel fast bind found stale state outside lock; falling back to locked resolve. unique_key=%s err=%s attempt=%s/%s",
                unique_key,
                fast_bind_err,
                attempt_idx + 1,
                max_attempts,
            )

        try:
            lock_token = _acquire_lock(key=lock_key, ttl_seconds=120)
        except Exception as e:  # noqa: BLE001
            return Result[Union[MPSCChanProducer, MPSCChanConsumer, MPMCChanProducer, MPMCChanConsumer], ApiError].new_error(
                EtcdError(
                    message=f"Failed to acquire etcd lock: {lock_key}, err={e}",
                    component="api_ext_chan.new_or_bind_with_unique_key",
                    transport=TransportName.GRPC,
                    transport_user=TransportUser.ETCD,
                )
            )

        try:
            first_res = _call_resolve_under_lock()
            if first_res.is_ok():
                final_value = first_res.unwrap()
                final_error = None
            else:
                err = first_res.unwrap_error()
                logging.warning(
                    "new_or_bind_with_unique_key retry once under lock: unique_key=%s err=%s",
                    unique_key,
                    err,
                )
                second_res = _call_resolve_under_lock()
                if second_res.is_ok():
                    final_value = second_res.unwrap()
                    final_error = None
                else:
                    final_value = None
                    final_error = second_res.unwrap_error()
        finally:
            _release_lock(key=lock_key, token=lock_token)

        if final_value is None:
            assert final_error is not None
            return Result[Union[MPSCChanProducer, MPSCChanConsumer, MPMCChanProducer, MPMCChanConsumer], ApiError].new_error(final_error)

        if not (isinstance(final_value, tuple) and len(final_value) == 2 and final_value[0] == "bind_existing"):
            return Result[Union[MPSCChanProducer, MPSCChanConsumer, MPMCChanProducer, MPMCChanConsumer], ApiError].new_ok(final_value)

        bind_chan_id = final_value[1]
        bind_res = _bind_existing_channel(bind_chan_id)
        if bind_res.is_ok():
            return bind_res

        bind_err = bind_res.unwrap_error()
        if not _is_stale_bind_error(bind_err):
            return Result[Union[MPSCChanProducer, MPSCChanConsumer, MPMCChanProducer, MPMCChanConsumer], ApiError].new_error(bind_err)

        logging.warning(
            "existing channel bind failed with stale state outside lock; cleanup and retry. unique_key=%s chan_id=%s err=%s attempt=%s/%s",
            unique_key,
            bind_chan_id,
            bind_err,
            attempt_idx + 1,
            max_attempts,
        )
        try:
            cleanup_lock_token = _acquire_lock(key=lock_key, ttl_seconds=120)
        except Exception as e:  # noqa: BLE001
            return Result[Union[MPSCChanProducer, MPSCChanConsumer, MPMCChanProducer, MPMCChanConsumer], ApiError].new_error(
                EtcdError(
                    message=f"Failed to reacquire etcd lock for stale cleanup: {lock_key}, err={e}",
                    component="api_ext_chan.new_or_bind_with_unique_key",
                    transport=TransportName.GRPC,
                    transport_user=TransportUser.ETCD,
                )
            )
        try:
            cleanup_res = _cleanup_stale_mapping_under_lock(expected_chan_id=bind_chan_id)
        finally:
            _release_lock(key=lock_key, token=cleanup_lock_token)
        if not cleanup_res.is_ok():
            return Result[Union[MPSCChanProducer, MPSCChanConsumer, MPMCChanProducer, MPMCChanConsumer], ApiError].new_error(
                cleanup_res.unwrap_error()
            )

        final_value = None
        final_error = bind_err

    assert final_error is not None
    return Result[Union[MPSCChanProducer, MPSCChanConsumer, MPMCChanProducer, MPMCChanConsumer], ApiError].new_error(final_error)


def new_etcd_client(api: KvClient) -> Result[etcd3.Etcd3Client, ApiError]:
    """Create etcd client"""
    etcd_config = api.get_etcd_config()
    first_address = etcd_config[0]
    host, port = first_address.split(":")
    print(f"new_etcd_client: {host}:{port}")
    try:
        return Result.new_ok(etcd3.client(host=host, port=int(port)))
    except Exception as e:
        return Result.new_error(
            EtcdError(
                message=(
                    f"Failed to create etcd grpc client for endpoint {first_address}: {type(e).__name__}: {e}"
                ),
                component="api_ext_chan.new_etcd_client",
                transport=TransportName.GRPC,
                transport_user=TransportUser.ETCD,
            )
        )



# c-style apis - unified registry for both MPSC and MPMC
CHANID_2_NODES: Dict[str, Union[MPSCChanProducer, MPSCChanConsumer, MPMCChanProducer, MPMCChanConsumer]] = {}

def get_chan_by_id(chan_type: ChanType, chan_id: str) -> Result[Union[MPSCChanProducer, MPSCChanConsumer, MPMCChanProducer, MPMCChanConsumer], ApiError]:
    if not isinstance(chan_id, str) or not chan_id.isdigit():
        return Result[Union[MPSCChanProducer, MPSCChanConsumer, MPMCChanProducer, MPMCChanConsumer], ApiError].new_error(
            ChanKeyNotFoundError(
                message=f"invalid chan_id({chan_id!r}) for chan_type({chan_type.value})",
            )
        )
    key=f"{chan_type.value}_{chan_id}"
    if key not in CHANID_2_NODES:
        return Result[Union[MPSCChanProducer, MPSCChanConsumer, MPMCChanProducer, MPMCChanConsumer], ApiError].new_error(
            ChanKeyNotFoundError(
                message=f"(chan_id({chan_id}), chan_type({chan_type.value})) not found.",
            )
        )
    return Result(CHANID_2_NODES[key])
    

def set_chan_by_id(chan_type: ChanType, chan_id: str, chan: Union[MPSCChanProducer, MPSCChanConsumer, MPMCChanProducer, MPMCChanConsumer]):
    if not isinstance(chan_id, str) or not chan_id.isdigit():
        raise ValueError(f"invalid chan_id: {chan_id!r}")
    key=f"{chan_type.value}_{chan_id}"
    CHANID_2_NODES[key] = chan

def del_chan_by_id(chan_type: ChanType, chan_id: str):
    if not isinstance(chan_id, str) or not chan_id.isdigit():
        raise ValueError(f"invalid chan_id: {chan_id!r}")
    key=f"{chan_type.value}_{chan_id}"
    del CHANID_2_NODES[key]


def chan_new(
    api: KvClient,
    chan_config: Dict[str, int],
    chan_type: ChanType,
    chan_role: ChanRole,
    etcd_client: Optional[etcd3.Etcd3Client] = None,
) -> Result[str, ApiError]:
    if chan_type == ChanType.MPSC:
        if chan_role == ChanRole.PRODUCER:
            producer = MPSCChanProducer(api, None, chan_config, etcd_client)
            if not isinstance(producer.chan_id, str) or not producer.chan_id.isdigit():
                return Result[str, ApiError].new_error(
                    InvalidConfigurationError(
                        message="MPSC producer initialize error!",
                    )
                )
            CHANID_2_NODES[chan_type.value + "_" + producer.chan_id] = producer
            return Result(producer.chan_id)
        elif chan_role == ChanRole.CONSUMER:
            consumer = MPSCChanConsumer(api, None, chan_config, etcd_client)
            if not isinstance(consumer.chan_id, str) or not consumer.chan_id.isdigit():
                return Result[str, ApiError].new_error(
                    InvalidConfigurationError(
                        message="MPSC consumer initialize error!",
                    )
                )
            CHANID_2_NODES[chan_type.value + "_" + consumer.chan_id] = consumer
            return Result(consumer.chan_id)
        else:
            return Result[str, ApiError].new_error(
                InvalidConfigurationError(
                    message="Invalid MPSC channel role",
                )
            )
    elif chan_type == ChanType.MPMC:
        if chan_role == ChanRole.PRODUCER:
            producer = MPMCChanProducer(api, None, chan_config, etcd_client)
            if not isinstance(producer.mpmc_id, str) or not producer.mpmc_id.isdigit():
                return Result[str, ApiError].new_error(
                    InvalidConfigurationError(
                        message="MPMC producer initialize error!",
                    )
                )
            CHANID_2_NODES[chan_type.value + "_" + producer.mpmc_id] = producer
            return Result(producer.mpmc_id)
        elif chan_role == ChanRole.CONSUMER:
            consumer = MPMCChanConsumer(api, None, chan_config, etcd_client)
            if not isinstance(consumer.mpmc_id, str) or not consumer.mpmc_id.isdigit():
                return Result[str, ApiError].new_error(
                    InvalidConfigurationError(
                        message="MPMC consumer initialize error!",
                    )
                )
            CHANID_2_NODES[chan_type.value + "_" + consumer.mpmc_id] = consumer
            return Result(consumer.mpmc_id)
        else:
            return Result[str, ApiError].new_error(
                InvalidConfigurationError(
                    message="Invalid MPMC channel role",
                )
            )
    else:
        return Result[str, ApiError].new_error(
            InvalidConfigurationError(
                message="Invalid channel type",
            )
        )


def _construct_bound_channel(
    api: KvClient,
    chan_config: Dict[str, int],
    chan_id: str,
    chan_type: ChanType,
    chan_role: ChanRole,
    etcd_client: Optional[etcd3.Etcd3Client] = None,
) -> Result[
    Union[MPSCChanProducer, MPSCChanConsumer, MPMCChanProducer, MPMCChanConsumer],
    ApiError,
]:
    """Construct one bound channel without publishing it through the process registry."""

    if chan_type == ChanType.MPSC:
        if chan_role == ChanRole.PRODUCER:
            try:
                producer = MPSCChanProducer(api, chan_id, chan_config, etcd_client)
            except (InvalidConfigurationError, PayloadLeaseNotFoundError) as e:
                return Result.new_error(e)
            except Exception as e:
                return Result.new_error(
                    InvalidConfigurationError(
                        message=f"MPSC producer initialize error! {e}",
                    )
                )
            if not isinstance(producer.chan_id, str) or not producer.chan_id.isdigit():
                return Result.new_error(
                    InvalidConfigurationError(
                        message="MPSC producer initialize error!",
                    )
                )
            return Result.new_ok(producer)
        elif chan_role == ChanRole.CONSUMER:
            try:
                consumer = MPSCChanConsumer(api, chan_id, chan_config, etcd_client)
            except (InvalidConfigurationError, PayloadLeaseNotFoundError) as e:
                return Result.new_error(e)
            except Exception as e:
                return Result.new_error(
                    InvalidConfigurationError(
                        message=f"MPSC consumer initialize error! {e}",
                    )
                )
            if not isinstance(consumer.chan_id, str) or not consumer.chan_id.isdigit():
                return Result.new_error(
                    InvalidConfigurationError(
                        message="MPSC consumer initialize error!",
                    )
                )
            return Result.new_ok(consumer)
        else:
            return Result.new_error(
                InvalidConfigurationError(
                    message="Invalid MPSC channel role",
                )
            )
    elif chan_type == ChanType.MPMC:
        if chan_role == ChanRole.PRODUCER:
            try:
                producer = MPMCChanProducer(api, chan_id, chan_config, etcd_client)
            except InvalidConfigurationError as e:
                if "lease not found" in str(e).lower() or "payload lease" in str(e).lower():
                    return Result.new_error(
                        InvalidConfigurationError(
                            message=str(e),
                            config_key="mpmc_meta_stale",
                        )
                    )
                return Result.new_error(e)
            except Exception as e:
                return Result.new_error(
                    InvalidConfigurationError(
                        message=f"MPMC producer initialize error! {e}",
                    )
                )
            if not isinstance(producer.mpmc_id, str) or not producer.mpmc_id.isdigit():
                return Result.new_error(
                    InvalidConfigurationError(
                        message="MPMC producer initialize error!",
                    )
                )
            return Result.new_ok(producer)
        elif chan_role == ChanRole.CONSUMER:
            try:
                consumer = MPMCChanConsumer(api, chan_id, chan_config, etcd_client)
            except InvalidConfigurationError as e:
                if "lease not found" in str(e).lower() or "payload lease" in str(e).lower():
                    return Result.new_error(
                        InvalidConfigurationError(
                            message=str(e),
                            config_key="mpmc_meta_stale",
                        )
                    )
                return Result.new_error(e)
            except Exception as e:
                return Result.new_error(
                    InvalidConfigurationError(
                        message=f"MPMC consumer initialize error! {e}",
                    )
                )
            if not isinstance(consumer.mpmc_id, str) or not consumer.mpmc_id.isdigit():
                return Result.new_error(
                    InvalidConfigurationError(
                        message="MPMC consumer initialize error!",
                    )
                )
            return Result.new_ok(consumer)
        else:
            return Result.new_error(
                InvalidConfigurationError(
                    message="Invalid MPMC channel role",
                )
            )
    else:
        return Result.new_error(
            InvalidConfigurationError(
                message="Invalid channel type or role",
            )
        )


def chan_bind(
    api: KvClient,
    chan_config: Dict[str, int],
    chan_id: str,
    chan_type: ChanType,
    chan_role: ChanRole,
    etcd_client: Optional[etcd3.Etcd3Client] = None,
) -> Result[bool, ApiError]:
    """Bind a channel and publish it through the legacy process registry."""

    result = _construct_bound_channel(
        api,
        chan_config,
        chan_id,
        chan_type,
        chan_role,
        etcd_client,
    )
    if not result.is_ok():
        return Result.new_error(result.unwrap_error())
    set_chan_by_id(chan_type, chan_id, result.unwrap())
    return Result.new_ok(True)


def chan_unbind(chan_type: ChanType, chan_id: str) -> Result[bool, ApiError]:
    result = get_chan_by_id(chan_type, chan_id)
    if not result.is_ok():
        return Result[bool, ApiError].new_error(
            ChanKeyNotFoundError(
                message=f"chan_id: {chan_id} not found.",
            )
        )
    close_res = result.unwrap().close()
    if not close_res.is_ok():
        return Result[bool, ApiError].new_error(close_res.unwrap_error())
    # consume ok branch
    _ = close_res.unwrap()
    del_chan_by_id(chan_type, chan_id)
    
    return Result(True)


# cstyle mq ops (get/put) removed by request; prefer using class methods directly






__all__ = [
    "MPSCChanProducer", 
    "MPSCChanConsumer", 
    "MPMCChanProducer", 
    "MPMCChanConsumer",
    "ChanType",
    "ChanRole", 
    "CHANID_2_NODES",
    "new_or_bind_with_unique_key",
    "new_etcd_client",
    "chan_new",
    "chan_bind", 
    "chan_unbind",
]
