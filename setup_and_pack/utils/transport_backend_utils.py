from __future__ import annotations

TRANSPORT_BACKENDS: tuple[str, ...] = ("fastws", "tquic", "sockudo_ws", "tcp", "tcp_thread")
RDMA_BACKENDS: tuple[str, ...] = ("closed_sdk",)
PUBLIC_TRANSPORT_BACKEND = "tcp_thread"
PUBLIC_TRANSPORT_PROFILE_ID = "fluxon_tcp_thread"
TRANSPORT_PROFILE_IDS: dict[str, str] = {
    "fastws": "fluxon_fastws",
    "tquic": "fluxon_tquic",
    "sockudo_ws": "fluxon_sockudo_ws",
    "tcp": "fluxon_tcp",
    "tcp_thread": "fluxon_tcp_thread",
}

__all__ = [
    "TRANSPORT_BACKENDS",
    "RDMA_BACKENDS",
    "PUBLIC_TRANSPORT_BACKEND",
    "PUBLIC_TRANSPORT_PROFILE_ID",
    "TRANSPORT_PROFILE_IDS",
]
