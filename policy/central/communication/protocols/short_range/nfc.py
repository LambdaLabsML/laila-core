"""NFC communication transport (datagram radio).

Carries fragmented JSON-RPC over NFC peer/reader exchanges via
``nfcpy``. Built on the lazy datagram carrier.

- ``protocol_name`` ``"nfc"``
- URI scheme ``nfc://<target>``
"""

from __future__ import annotations

from typing import ClassVar

from .._carriers.lazy import _LazyDatagramRPCProtocol


class _LAILA_IDENTIFIABLE_NFC_COMM_PROTOCOL(_LazyDatagramRPCProtocol):
    """NFC transport."""

    protocol_name: ClassVar[str] = "nfc"
    _TOKEN_ALIASES: ClassVar[frozenset[str]] = frozenset({"nfc"})
    _DRIVER_MODULES: ClassVar[tuple] = ("nfc",)
    _DRIVER_EXTRA: ClassVar[str] = "nfc"

    @classmethod
    def can_handle_uri(cls, uri: str) -> bool:
        """Claim ``nfc://`` URIs."""
        return uri.startswith("nfc://")
