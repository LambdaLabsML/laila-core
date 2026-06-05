"""HTTP/2 communication transport.

Carries JSON-RPC over a long-lived HTTP/2 stream via ``h2``. Built on
the point-to-point stream carrier; the driver is imported on start and a
missing library raises a clear capability error.

- ``protocol_name`` ``"http2"`` (aliases ``h2``)
- URI scheme ``http2://host:port``
"""

from __future__ import annotations

import asyncio
from typing import Any, ClassVar

from .._carriers.p2p import _P2PStreamRPCProtocol


class _LAILA_IDENTIFIABLE_HTTP2_COMM_PROTOCOL(_P2PStreamRPCProtocol):
    """HTTP/2 stream transport."""

    protocol_name: ClassVar[str] = "http2"
    _TOKEN_ALIASES: ClassVar[frozenset[str]] = frozenset({"http2", "h2"})

    @classmethod
    def matches_token(cls, token: str) -> bool:
        """Accept ``"http2"`` / ``"h2"``."""
        return token.lower() in cls._TOKEN_ALIASES

    @classmethod
    def can_handle_uri(cls, uri: str) -> bool:
        """Claim ``http2://`` URIs."""
        return uri.startswith("http2://")

    async def _open_stream(self) -> tuple[asyncio.StreamReader, Any]:
        self._require_drivers(("h2",), "http")
        raise RuntimeError(
            "HTTP/2 transport requires an established h2 connection/stream; "
            "configure the HTTP/2 endpoint before peering."
        )
