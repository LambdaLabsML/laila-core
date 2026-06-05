"""Zigbee communication transport (datagram radio).

Carries fragmented JSON-RPC over a Zigbee network via ``zigpy`` + a
radio coprocessor. Built on the lazy datagram carrier: the driver is
imported on start and a missing library raises a clear capability error.

- ``protocol_name`` ``"zigbee"``
- URI scheme ``zigbee://<node>``
"""

from __future__ import annotations

from typing import ClassVar

from .._carriers.lazy import _LazyDatagramRPCProtocol


class _LAILA_IDENTIFIABLE_ZIGBEE_COMM_PROTOCOL(_LazyDatagramRPCProtocol):
    """Zigbee 802.15.4 transport."""

    protocol_name: ClassVar[str] = "zigbee"
    _TOKEN_ALIASES: ClassVar[frozenset[str]] = frozenset({"zigbee"})
    _DRIVER_MODULES: ClassVar[tuple] = ("zigpy",)
    _DRIVER_EXTRA: ClassVar[str] = "zigbee"

    @classmethod
    def can_handle_uri(cls, uri: str) -> bool:
        """Claim ``zigbee://`` URIs."""
        return uri.startswith("zigbee://")
