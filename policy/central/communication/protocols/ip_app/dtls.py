"""DTLS communication transport (datagram, secured).

DTLS secures a UDP datagram channel (the basis of secure CoAP). It
requires a DTLS-capable stack (OpenSSL DTLS bindings), which the stdlib
does not expose, so this lazy datagram transport raises a clear
requirement error on start.

- ``protocol_name`` ``"dtls"``
- URI scheme ``dtls://host:port``
"""

from __future__ import annotations

from typing import ClassVar

from .._carriers.lazy import _LazyDatagramRPCProtocol


class _LAILA_IDENTIFIABLE_DTLS_COMM_PROTOCOL(_LazyDatagramRPCProtocol):
    """DTLS-secured datagram transport."""

    protocol_name: ClassVar[str] = "dtls"
    _TOKEN_ALIASES: ClassVar[frozenset[str]] = frozenset({"dtls"})

    @classmethod
    def can_handle_uri(cls, uri: str) -> bool:
        """Claim ``dtls://`` URIs."""
        return uri.startswith("dtls://")
