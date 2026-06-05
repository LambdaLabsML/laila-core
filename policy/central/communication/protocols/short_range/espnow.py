"""ESP-NOW communication transport (datagram radio).

ESP-NOW is a connectionless peer-to-peer protocol that runs on ESP
Wi-Fi firmware; there is no general host-side Python driver, so this
lazy datagram transport raises a clear hardware-requirement error on
start (the device is an ESP running ESP-NOW firmware).

- ``protocol_name`` ``"esp-now"`` (aliases ``espnow``)
- URI scheme ``espnow://<mac>``
"""

from __future__ import annotations

from typing import ClassVar

from .._carriers.lazy import _LazyDatagramRPCProtocol


class _LAILA_IDENTIFIABLE_ESPNOW_COMM_PROTOCOL(_LazyDatagramRPCProtocol):
    """ESP-NOW transport (requires ESP firmware)."""

    protocol_name: ClassVar[str] = "esp-now"
    _TOKEN_ALIASES: ClassVar[frozenset[str]] = frozenset({"esp-now", "espnow"})

    @classmethod
    def matches_token(cls, token: str) -> bool:
        """Accept ``"esp-now"`` / ``"espnow"``."""
        return token.lower() in cls._TOKEN_ALIASES

    @classmethod
    def can_handle_uri(cls, uri: str) -> bool:
        """Claim ``espnow://`` URIs."""
        return uri.startswith("espnow://")
