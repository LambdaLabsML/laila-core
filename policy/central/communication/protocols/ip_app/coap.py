"""CoAP communication transport (datagram).

Carries fragmented JSON-RPC over CoAP (UDP) via ``aiocoap``. Built on
the lazy datagram carrier; the driver is imported on start and a missing
library raises a clear capability error.

- ``protocol_name`` ``"coap"`` (aliases ``coaps``)
- URI scheme ``coap://host:port`` (also ``coaps://``)
"""

from __future__ import annotations

from typing import ClassVar

from .._carriers.lazy import _LazyDatagramRPCProtocol


class _LAILA_IDENTIFIABLE_COAP_COMM_PROTOCOL(_LazyDatagramRPCProtocol):
    """CoAP transport."""

    protocol_name: ClassVar[str] = "coap"
    _TOKEN_ALIASES: ClassVar[frozenset[str]] = frozenset({"coap", "coaps"})
    _DRIVER_MODULES: ClassVar[tuple] = ("aiocoap",)
    _DRIVER_EXTRA: ClassVar[str] = "coap"

    @classmethod
    def matches_token(cls, token: str) -> bool:
        """Accept ``"coap"`` / ``"coaps"``."""
        return token.lower() in cls._TOKEN_ALIASES

    @classmethod
    def can_handle_uri(cls, uri: str) -> bool:
        """Claim ``coap://`` and ``coaps://`` URIs."""
        return uri.startswith("coap://") or uri.startswith("coaps://")
