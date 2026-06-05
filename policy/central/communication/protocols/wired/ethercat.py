"""EtherCAT (industrial fieldbus) communication transport (datagram).

Carries fragmented JSON-RPC over EtherCAT via the SOEM master
(``pysoem``) on a dedicated NIC. Built on the lazy datagram carrier.

- ``protocol_name`` ``"ethercat"``
- URI scheme ``ethercat://<slave>``
"""

from __future__ import annotations

from typing import ClassVar

from .._carriers.lazy import _LazyDatagramRPCProtocol


class _LAILA_IDENTIFIABLE_ETHERCAT_COMM_PROTOCOL(_LazyDatagramRPCProtocol):
    """EtherCAT fieldbus transport."""

    protocol_name: ClassVar[str] = "ethercat"
    _TOKEN_ALIASES: ClassVar[frozenset[str]] = frozenset({"ethercat"})
    _DRIVER_MODULES: ClassVar[tuple] = ("pysoem",)
    _DRIVER_EXTRA: ClassVar[str] = "ethercat"

    @classmethod
    def can_handle_uri(cls, uri: str) -> bool:
        """Claim ``ethercat://`` URIs."""
        return uri.startswith("ethercat://")
