"""Satellite (Iridium SBD / Swarm) communication transport.

Satellite short-burst-data modems (Iridium SBD, Swarm) are reached
through a serial AT interface, so this subclasses the serial (UART)
transport. Payloads are small and costly; the caller is responsible for
keeping messages within the modem's limits.

- ``protocol_name`` ``"satellite"`` (aliases ``iridium`` / ``swarm`` / ``sbd``)
- URI scheme ``satellite:///dev/ttyUSB0``
"""

from __future__ import annotations

from typing import ClassVar

from ..wired.uart import _LAILA_IDENTIFIABLE_UART_COMM_PROTOCOL


class _LAILA_IDENTIFIABLE_SATELLITE_COMM_PROTOCOL(_LAILA_IDENTIFIABLE_UART_COMM_PROTOCOL):
    """Satellite SBD transport (serial AT modem)."""

    protocol_name: ClassVar[str] = "satellite"
    _TOKEN_ALIASES: ClassVar[frozenset[str]] = frozenset(
        {"satellite", "iridium", "swarm", "sbd"}
    )

    @classmethod
    def can_handle_uri(cls, uri: str) -> bool:
        """Claim ``satellite://`` URIs."""
        return uri.startswith("satellite://")
