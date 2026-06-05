"""Ethernet (incl. PoE) communication transport.

Ethernet is an IP carrier: once the link is up the data path is plain
TCP, so this transport subclasses the raw-TCP transport and only adds
link identity (the bound NIC) and its own token/URI. Physical link
bring-up (cable/PoE, DHCP) is handled by the OS; ``interface`` is
recorded for diagnostics and future link-management.

- ``protocol_name`` ``"ethernet"`` (aliases ``eth`` / ``poe``)
- URI scheme ``ethernet://host:port``
"""

from __future__ import annotations

from typing import ClassVar

from pydantic import Field

from .tcp import _LAILA_IDENTIFIABLE_TCP_COMM_PROTOCOL


class _LAILA_IDENTIFIABLE_ETHERNET_COMM_PROTOCOL(_LAILA_IDENTIFIABLE_TCP_COMM_PROTOCOL):
    """Ethernet/PoE transport (TCP data path over a wired NIC)."""

    protocol_name: ClassVar[str] = "ethernet"
    _TOKEN_ALIASES: ClassVar[frozenset[str]] = frozenset({"ethernet", "eth", "poe"})

    #: Optional NIC to bind/diagnose (e.g. ``"eth0"``). Informational;
    #: the socket still binds by ``host``.
    interface: str | None = Field(default=None)

    @classmethod
    def can_handle_uri(cls, uri: str) -> bool:
        """Claim ``ethernet://`` URIs."""
        return uri.startswith("ethernet://")

    def connect_ethernet(self, host: str, port: int, secret: str) -> str:
        """Convenience wrapper building the ``ethernet://`` URI."""
        return self.connect(f"ethernet://{host}:{port}", secret)
