"""LoRaWAN communication transport (datagram radio).

LoRaWAN end-devices are modems/firmware joined to a network server;
there is no general host-side Python driver to bring up automatically,
so this lazy datagram transport raises a clear hardware/infrastructure
requirement error on start (a LoRaWAN modem + network server/ChirpStack
are required).

- ``protocol_name`` ``"lorawan"``
- URI scheme ``lorawan://<deveui>``
"""

from __future__ import annotations

from typing import ClassVar

from .._carriers.lazy import _LazyDatagramRPCProtocol


class _LAILA_IDENTIFIABLE_LORAWAN_COMM_PROTOCOL(_LazyDatagramRPCProtocol):
    """LoRaWAN transport (modem + network server)."""

    protocol_name: ClassVar[str] = "lorawan"
    _TOKEN_ALIASES: ClassVar[frozenset[str]] = frozenset({"lorawan"})

    @classmethod
    def can_handle_uri(cls, uri: str) -> bool:
        """Claim ``lorawan://`` URIs."""
        return uri.startswith("lorawan://")
