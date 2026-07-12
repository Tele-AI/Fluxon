from __future__ import annotations

"""MPMC/MQ helpers extracted from distributed_benchmark_node.

Responsibilities:
- Hold MQ-related state (role/weight/config/seed, etc.)
- Parse MQ config (from coordinator-provided config["mq"])
- Build messages with per-message seed and producer_id
- Decode and verify payload on the consumer side
"""

from dataclasses import dataclass
from enum import Enum, unique
from typing import Any, Dict, Optional, Tuple
import json
import random
import logging

from fluxon_py.kvclient.kvclient_interface import KvClient
from fluxon_py.api_error import (
    ChannelClosedError,
    MessageConsumptionNoNewMessageError,
    ProducerClosedError,
)
from fluxon_py.api_ext_chan import (
    ChanType,
    ChanRole,
    new_or_bind_with_unique_key,
    MPMCChanConsumer,
    MPMCChanProducer,
)

logger = logging.getLogger("benchmark_node_mq")

MQ_MESSAGE_KIND_BENCHMARK = "benchmark"
MQ_MESSAGE_KIND_PREFEED = "prefeed"
MQ_MESSAGE_KINDS = frozenset(
    {
        MQ_MESSAGE_KIND_BENCHMARK,
        MQ_MESSAGE_KIND_PREFEED,
    }
)


@dataclass
class MQState:
    """Holds MQ-related state for a benchmark node."""

    role: Optional[str] = None
    weight: float = 1.0
    config: Dict[str, Any] = None  # full mq config (mq_base + per-node patch + role/weight)
    chan_config: Dict[str, Any] = None  # config passed to new_or_bind_with_unique_key
    seed_base: Optional[int] = None
    seq_counter: int = 0
    producer_id: Optional[str] = None

    def __post_init__(self) -> None:
        if self.config is None:
            self.config = {}
        if self.chan_config is None:
            self.chan_config = {}


def apply_mq_config_from_test_config(
    mq_state: MQState, mq_cfg: Dict[str, Any], default_chan_config: Dict[str, Any]
) -> None:
    """Update mq_state and chan_config from coordinator-provided config["mq"].

    - mq_cfg: coordinator-filled dict, at least contains capacity/ttl_seconds/weight/role
    - default_chan_config: a copy of CHAN_CONFIG
    """
    if not isinstance(mq_cfg, dict):
        return

    mq_state.config = mq_cfg

    role = mq_cfg.get("role")
    if isinstance(role, str) and role.strip():
        mq_state.role = role.strip()
    raw_weight = mq_cfg.get("weight")
    if raw_weight is not None:
        mq_state.weight = float(raw_weight)

    cap = mq_cfg.get("capacity")
    ttl = mq_cfg.get("ttl_seconds")
    chan = dict(default_chan_config)
    if cap is not None:
        chan["capacity"] = int(cap)
    if ttl is not None:
        chan["ttl_seconds"] = int(ttl)
    mq_state.chan_config = chan


def _next_seed(mq_state: MQState) -> int:
    """Generate per-message seed from node-level seed base and sequence counter."""
    if mq_state.seed_base is None:
        mq_state.seed_base = random.SystemRandom().randint(0, 2**63 - 1)
    seed = (int(mq_state.seed_base) + mq_state.seq_counter) & ((1 << 63) - 1)
    mq_state.seq_counter += 1
    return seed


def _encode_header(seed: int, producer_id: str, seq: int, message_kind: str) -> bytes:
    """Encode the strongly typed MQ benchmark header."""
    if message_kind not in MQ_MESSAGE_KINDS:
        raise ValueError(f"unsupported MQ message_kind: {message_kind!r}")
    header_obj = {
        "seed": int(seed),
        "producer_id": producer_id,
        "seq": int(seq),
        "message_kind": message_kind,
    }
    return (json.dumps(header_obj, separators=(",", ":"), ensure_ascii=False) + "\n").encode(
        "utf-8"
    )


def _generate_payload(seed: int, size: int) -> bytes:
    """Generate fixed-size payload from seed (same logic for producer/consumer)."""
    rng = random.Random(int(seed))
    return rng.randbytes(size)


def build_message(
    mq_state: MQState,
    value_size: int,
    fallback_producer_id: str,
    *,
    message_kind: str = MQ_MESSAGE_KIND_BENCHMARK,
) -> Dict[str, Any]:
    """Build one flat MPMC payload record for put_data().

    The channel API requires a flat dict payload. We keep the benchmark wire
    format inside the bytes payload so producer and consumer can still verify
    message integrity deterministically.
    """
    producer_id = mq_state.producer_id or fallback_producer_id
    seq = mq_state.seq_counter
    seed = _next_seed(mq_state)
    header = _encode_header(seed, producer_id, seq, message_kind)
    if value_size <= len(header):
        raise ValueError(
            f"value_size({value_size}) too small for MQ header({len(header)})"
        )
    payload_size = value_size - len(header)
    payload = _generate_payload(seed, payload_size)
    unique_id = f"{producer_id}:{seq}:{seed}"
    return {
        "unique_id": unique_id,
        "payload": header + payload,
    }


def _decode_message(raw: bytes) -> tuple[Dict[str, Any], bytes]:
    """Parse MQ message; return (header_dict, payload_bytes)."""
    try:
        header_bytes, payload = raw.split(b"\n", 1)
    except ValueError as exc:
        raise ValueError("MQ message missing header separator") from exc
    try:
        header = json.loads(header_bytes.decode("utf-8"))
    except Exception as exc:  # noqa: BLE001
        raise ValueError(f"MQ header JSON decode failed: {exc}") from exc
    return header, payload


def _verify_message(raw: bytes) -> tuple[bool, str, int, Optional[str]]:
    """Decode and verify one MQ message.

    Returns (ok, error_msg, data_size, message_kind).
    """
    if not raw:
        return False, "empty MPMC payload", 0, None

    try:
        header, payload = _decode_message(raw)
        message_kind = header.get("message_kind")
        if message_kind not in MQ_MESSAGE_KINDS:
            raise ValueError(f"invalid MQ message_kind: {message_kind!r}")
        seed = int(header.get("seed"))
        expected = _generate_payload(seed, len(payload))
        if payload != expected:
            return False, "MQ payload verification failed", len(payload), str(message_kind)
    except Exception as exc:  # noqa: BLE001
        return False, f"MQ decode/verify error: {exc}", len(raw), None

    return True, "", len(raw), str(message_kind)


@dataclass(frozen=True)
class ClusterInfoSnapshot:
    """Complete producer-visible MPMC topology observation."""

    mpmc_id: str
    active_consumers: int
    ready_channel_ids: Tuple[str, ...]

    @property
    def ready_channels(self) -> int:
        """Return the number of authoritative ready channel IDs."""
        return len(self.ready_channel_ids)


def get_cluster_info_snapshot(
    endpoint: MPMCChanProducer | MPMCChanConsumer,
) -> ClusterInfoSnapshot:
    """Read one complete MPMC topology observation from a typed endpoint."""
    if not isinstance(endpoint, (MPMCChanProducer, MPMCChanConsumer)):
        raise TypeError(
            "MPMC topology snapshot requires MPMCChanProducer or MPMCChanConsumer, "
            f"got {type(endpoint).__name__}"
        )

    channel = endpoint.mpmc_channel
    if channel is None:
        raise RuntimeError("MPMC topology snapshot requires an open endpoint")

    ready_channel_ids = channel.get_remote_ready_channels()
    for channel_id in ready_channel_ids:
        if not isinstance(channel_id, str) or not channel_id.isdigit():
            raise ValueError(f"invalid ready MPSC channel ID: {channel_id!r}")

    return ClusterInfoSnapshot(
        mpmc_id=endpoint.get_chan_id(),
        active_consumers=len(channel.get_active_member_ids(ChanRole.CONSUMER)),
        ready_channel_ids=tuple(sorted(ready_channel_ids, key=int)),
    )


def init_mq_channel(
    role: str,
    kv_store: KvClient,
    chan_config: Dict[str, Any],
    unique_id: str,
    weight: float,
) -> Tuple[Optional[MPMCChanProducer], Optional[MPMCChanConsumer], Optional[str]]:
    """Initialize an MPMC channel by role (wraps new_or_bind_with_unique_key).

    Returns (producer, consumer, error_msg).
    Exactly one of producer/consumer is non-None on success; error_msg is None on success.
    """
    if kv_store is None:
        return None, None, "KV store is not initialized"

    role = (role or "").lower()
    # Copy config to avoid mutating the caller's chan_config.
    cfg: Dict[str, Any] = dict(chan_config)
    try:
        if role == "producer":
            # Producer must carry weight; if caller did not set it, use `weight`.
            try:
                cfg["weight"] = int(weight)
            except Exception as exc:  # noqa: BLE001
                return None, None, f"Invalid producer weight: {weight} ({exc})"

            result = new_or_bind_with_unique_key(
                api=kv_store,
                chan_config=cfg,
                unique_id=unique_id,
                chan_type=ChanType.MPMC,
                chan_role=ChanRole.PRODUCER,
            )
            if not result.is_ok():
                return None, None, f"Failed to init MPMC producer: {result.unwrap_error()}"
            obj = result.unwrap()
            if isinstance(obj, MPMCChanProducer):
                return obj, None, None
            return None, None, "Returned object is not MPMCChanProducer"

        if role == "consumer":
            if "weight" in cfg:
                logger.warning(
                    "MQ chan_config contains 'weight' but role is consumer; weight only applies to producer and will be ignored."
                )
                cfg.pop("weight", None)

            result = new_or_bind_with_unique_key(
                api=kv_store,
                chan_config=cfg,
                unique_id=unique_id,
                chan_type=ChanType.MPMC,
                chan_role=ChanRole.CONSUMER,
            )
            if not result.is_ok():
                return None, None, f"Failed to init MPMC consumer: {result.unwrap_error()}"
            obj = result.unwrap()
            if isinstance(obj, MPMCChanConsumer):
                return None, obj, None
            return None, None, "Returned object is not MPMCChanConsumer"

        return None, None, f"Unsupported MQ role: {role}"

    except Exception as exc:  # noqa: BLE001
        return None, None, f"Exception while initializing MPMC channel: {exc}"


class MQClosedError(RuntimeError):
    """MQ channel is explicitly closed for this benchmark run; used to stop upper-layer loops."""


@unique
class MQGetStatus(Enum):
    """Outcome class for a single MPMC GET attempt."""

    DATA = "DATA"
    NO_MESSAGE = "NO_MESSAGE"
    ERROR = "ERROR"


@dataclass(frozen=True)
class MQGetOutcome:
    """Typed outcome for a single MPMC GET attempt."""

    status: MQGetStatus
    ok: bool
    error_msg: str
    data_size: int
    message_kind: Optional[str] = None


def mq_put_to_channel_once(
    producer: MPMCChanProducer,
    channel_id: str,
    value: Dict[str, Any],
) -> Optional[str]:
    """Put one readiness message into one explicit ready MPSC channel."""
    if not isinstance(producer, MPMCChanProducer):
        return "MPMC producer is not initialized"
    if not isinstance(channel_id, str) or not channel_id.isdigit():
        return f"invalid MPSC channel_id: {channel_id!r}"

    try:
        ready_channel_ids = set(producer.mpmc_channel.get_remote_ready_channels())
        if channel_id not in ready_channel_ids:
            return f"MPSC channel is not ready for prefeed: channel_id={channel_id}"
        channel = producer._new_or_get_mpsc_producer(channel_id)
        result = channel.put_data(value)
        if not result.is_ok():
            err = result.unwrap_error()
            if isinstance(err, ProducerClosedError):
                return f"MPMC prefeed producer closed: channel_id={channel_id} err={err}"
            return f"MPMC prefeed PUT failed: channel_id={channel_id} err={err}"
        _ = result.unwrap()
        return None
    except Exception as exc:  # noqa: BLE001
        return f"MPMC prefeed PUT exception: channel_id={channel_id} err={exc}"


def mq_put_once(producer: MPMCChanProducer, value: Dict[str, Any]) -> Optional[str]:
    """Execute one put_data call; return error message or None.

    When ProducerClosedError is returned, raise MQClosedError to stop the upper loop.
    """

    if producer is None:
        return "MPMC producer is not initialized"

    try:
        result = producer.put_data(value)
        if not result.is_ok():
            err = result.unwrap_error()
            if isinstance(err, ProducerClosedError):
                # ProducerClosedError can be observed transiently when the topology is converging.
                # Treat it as a normal operation failure so the benchmark can keep making progress
                # and produce a deterministic result instead of exiting with zero samples.
                return f"MPMC PUT producer closed: {err}"
            return f"MPMC PUT failed: {err}"
        _ = result.unwrap()
        return None
    except MQClosedError:
        # Upper layer catches this to exit the loop.
        raise
    except Exception as exc:  # noqa: BLE001
        return f"MPMC PUT exception: {exc}"


def mq_get_once(consumer: MPMCChanConsumer, batch_size: int = 1) -> MQGetOutcome:
    """Execute one get_data + MQ verification.

    When ChannelClosedError is returned, raise MQClosedError to stop the upper loop.
    """

    if consumer is None:
        return MQGetOutcome(
            status=MQGetStatus.ERROR,
            ok=False,
            error_msg="MPMC consumer is not initialized",
            data_size=0,
        )

    try:
        # Explicitly set try_time to bound a single get_data call. This is an explicit
        # benchmark parameter (not a fallback default) to ensure retries have an upper
        # time bound so worker threads can continue progressing.
        result = consumer.get_data(batch_size=batch_size, try_time=5, prefetch_num=5)
        if not result.is_ok():
            err = result.unwrap_error()
            if isinstance(err, ChannelClosedError):
                # Similar to ProducerClosedError: allow the upper loop to continue collecting
                # samples (as failures) instead of exiting with an empty result set.
                return MQGetOutcome(
                    status=MQGetStatus.ERROR,
                    ok=False,
                    error_msg=f"MPMC GET channel closed: {err}",
                    data_size=0,
                )
            if isinstance(err, MessageConsumptionNoNewMessageError):
                return MQGetOutcome(
                    status=MQGetStatus.NO_MESSAGE,
                    ok=False,
                    error_msg=f"MPMC GET no message: {err}",
                    data_size=0,
                )
            return MQGetOutcome(
                status=MQGetStatus.ERROR,
                ok=False,
                error_msg=f"MPMC GET failed: {err}",
                data_size=0,
            )
        value_list = result.unwrap()
        if not value_list:
            return MQGetOutcome(
                status=MQGetStatus.ERROR,
                ok=False,
                error_msg="empty MPMC message batch",
                data_size=0,
            )
        item = value_list[0]
        if not isinstance(item, dict):
            return MQGetOutcome(
                status=MQGetStatus.ERROR,
                ok=False,
                error_msg=f"MPMC GET returned non-dict payload: {type(item).__name__}",
                data_size=0,
            )
        raw = item.get("payload")
        if not isinstance(raw, bytes):
            return MQGetOutcome(
                status=MQGetStatus.ERROR,
                ok=False,
                error_msg=f"MPMC GET payload field must be bytes, got: {type(raw).__name__}",
                data_size=0,
            )
        ok, msg, data_size, message_kind = _verify_message(raw)
        return MQGetOutcome(
            status=MQGetStatus.DATA,
            ok=ok,
            error_msg=msg,
            data_size=data_size,
            message_kind=message_kind,
        )
    except MQClosedError:
        # Upper layer catches this to exit the loop.
        raise
    except Exception as exc:  # noqa: BLE001
        return MQGetOutcome(
            status=MQGetStatus.ERROR,
            ok=False,
            error_msg=f"MPMC GET exception: {exc}",
            data_size=0,
        )
