from __future__ import annotations

from functools import wraps
import threading
from typing import Any, Callable, Dict, Optional

from ..logging import init_logger


logging = init_logger(__name__)


def publish_mq_construction(init: Callable[..., None]) -> Callable[..., None]:
    """Publish constructor completion to a KvClient-triggered close callback."""

    @wraps(init)
    def wrapped(self: Any, *args: Any, **kwargs: Any) -> None:
        construction_done = threading.Event()
        self._kv_child_construction_done = construction_done
        try:
            init(self, *args, **kwargs)
        finally:
            construction_done.set()

    return wrapped


class MqShutdownCtl:
    """Coordinate shutdown between one MQ owner and its inner operations."""

    def __init__(self) -> None:
        self.closed: bool = False
        self._op_lock = threading.Lock()
        self._callback_lock = threading.Lock()
        self._next_callback_id = 0
        self._close_callbacks: Dict[int, Callable[[], None]] = {}

    def register_construction_shutdown(
        self, callback: Callable[[], None]
    ) -> Callable[[], None]:
        """Register a shutdown signal owned by an in-flight inner construction."""

        with self._callback_lock:
            if self.closed:
                callback_id: Optional[int] = None
            else:
                callback_id = self._next_callback_id
                self._next_callback_id += 1
                self._close_callbacks[callback_id] = callback

        if callback_id is None:
            callback()

        def unregister() -> None:
            if callback_id is None:
                return
            with self._callback_lock:
                self._close_callbacks.pop(callback_id, None)

        return unregister

    def close(self) -> None:
        """Publish shutdown and notify registered in-flight constructions."""

        with self._callback_lock:
            self.closed = True
            callbacks = list(self._close_callbacks.values())
            self._close_callbacks.clear()

        for callback in callbacks:
            try:
                callback()
            except Exception as e:  # noqa: BLE001
                logging.warning("MQ shutdown callback failed: %s", e)
