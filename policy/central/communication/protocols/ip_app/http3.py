"""HTTP/3 (QUIC) communication transport (datagram).

Carries fragmented JSON-RPC over QUIC via ``aioquic``. Built on the lazy
datagram carrier.

- ``protocol_name`` ``"http3"`` (aliases ``h3`` / ``quic``)
- URI scheme ``http3://host:port`` (also ``quic://``)
"""

from __future__ import annotations

from typing import ClassVar

from .._carriers.lazy import _LazyDatagramRPCProtocol


class _LAILA_IDENTIFIABLE_HTTP3_COMM_PROTOCOL(_LazyDatagramRPCProtocol):
    """HTTP/3 (QUIC) transport."""

    protocol_name: ClassVar[str] = "http3"
    _TOKEN_ALIASES: ClassVar[frozenset[str]] = frozenset({"http3", "h3", "quic"})
    _DRIVER_MODULES: ClassVar[tuple] = ("aioquic",)
    _DRIVER_EXTRA: ClassVar[str] = "quic"

    @classmethod
    def matches_token(cls, token: str) -> bool:
        """Accept ``"http3"`` / ``"h3"`` / ``"quic"``."""
        return token.lower() in cls._TOKEN_ALIASES

    @classmethod
    def can_handle_uri(cls, uri: str) -> bool:
        """Claim ``http3://`` and ``quic://`` URIs."""
        return uri.startswith("http3://") or uri.startswith("quic://")
