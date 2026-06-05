"""RS-232 communication transport.

RS-232 is a serial-line electrical standard; at the byte level it is the
same asynchronous UART stream, so this transport is a thin subclass of
the UART transport with its own token/URI.

- ``protocol_name`` ``"rs232"`` (aliases ``rs-232``)
- URI scheme ``rs232:///dev/ttyS0``
"""

from __future__ import annotations

from typing import ClassVar

from .uart import _LAILA_IDENTIFIABLE_UART_COMM_PROTOCOL


class _LAILA_IDENTIFIABLE_RS232_COMM_PROTOCOL(_LAILA_IDENTIFIABLE_UART_COMM_PROTOCOL):
    """RS-232 serial transport."""

    protocol_name: ClassVar[str] = "rs232"
    _TOKEN_ALIASES: ClassVar[frozenset[str]] = frozenset({"rs232", "rs-232"})

    @classmethod
    def can_handle_uri(cls, uri: str) -> bool:
        """Claim ``rs232://`` URIs."""
        return uri.startswith("rs232://")
