"""LIN (Local Interconnect Network) communication transport.

LIN is a low-speed automotive serial bus; at the byte level it is a UART
stream, so this subclasses the serial transport with LIN-specific
tokens/URI.

- ``protocol_name`` ``"lin"``
- URI scheme ``lin:///dev/ttyUSB0``
"""

from __future__ import annotations

from typing import ClassVar

from .uart import _LAILA_IDENTIFIABLE_UART_COMM_PROTOCOL


class _LAILA_IDENTIFIABLE_LIN_COMM_PROTOCOL(_LAILA_IDENTIFIABLE_UART_COMM_PROTOCOL):
    """LIN automotive serial transport."""

    protocol_name: ClassVar[str] = "lin"
    _TOKEN_ALIASES: ClassVar[frozenset[str]] = frozenset({"lin"})

    @classmethod
    def can_handle_uri(cls, uri: str) -> bool:
        """Claim ``lin://`` URIs."""
        return uri.startswith("lin://")
