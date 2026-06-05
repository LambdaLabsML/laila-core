"""PROFINET (industrial fieldbus) communication transport (datagram).

Carries fragmented JSON-RPC over PROFINET RT via the ``p-net`` stack on
an Ethernet interface. Built on the lazy datagram carrier.

- ``protocol_name`` ``"profinet"``
- URI scheme ``profinet://<station>``
"""

from __future__ import annotations

from typing import ClassVar

from .._carriers.lazy import _LazyDatagramRPCProtocol


class _LAILA_IDENTIFIABLE_PROFINET_COMM_PROTOCOL(_LazyDatagramRPCProtocol):
    """PROFINET fieldbus transport."""

    protocol_name: ClassVar[str] = "profinet"
    _TOKEN_ALIASES: ClassVar[frozenset[str]] = frozenset({"profinet"})
    _DRIVER_MODULES: ClassVar[tuple] = ("pnet",)
    _DRIVER_EXTRA: ClassVar[str] = "profinet"

    @classmethod
    def can_handle_uri(cls, uri: str) -> bool:
        """Claim ``profinet://`` URIs."""
        return uri.startswith("profinet://")
