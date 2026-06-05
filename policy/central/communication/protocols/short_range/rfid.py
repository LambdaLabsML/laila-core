"""RFID communication transport.

Most RFID reader/writer modules present a serial (UART) interface, so
this subclasses the serial transport with RFID-specific tokens/URI.

- ``protocol_name`` ``"rfid"``
- URI scheme ``rfid:///dev/ttyUSB0``
"""

from __future__ import annotations

from typing import ClassVar

from ..wired.uart import _LAILA_IDENTIFIABLE_UART_COMM_PROTOCOL


class _LAILA_IDENTIFIABLE_RFID_COMM_PROTOCOL(_LAILA_IDENTIFIABLE_UART_COMM_PROTOCOL):
    """RFID transport (serial reader module)."""

    protocol_name: ClassVar[str] = "rfid"
    _TOKEN_ALIASES: ClassVar[frozenset[str]] = frozenset({"rfid"})

    @classmethod
    def can_handle_uri(cls, uri: str) -> bool:
        """Claim ``rfid://`` URIs."""
        return uri.startswith("rfid://")
