"""Sigfox communication transport.

Sigfox is an ultra-narrowband LPWAN reached through a serial AT modem,
so this subclasses the serial (UART) transport. Note Sigfox uplinks are
tiny (12 bytes) and rate-limited (~140 msgs/day); the caller is
responsible for keeping payloads within those limits.

- ``protocol_name`` ``"sigfox"``
- URI scheme ``sigfox:///dev/ttyUSB0`` (port comes from the ``port`` field)
"""

from __future__ import annotations

from typing import ClassVar

from ..wired.uart import _LAILA_IDENTIFIABLE_UART_COMM_PROTOCOL


class _LAILA_IDENTIFIABLE_SIGFOX_COMM_PROTOCOL(_LAILA_IDENTIFIABLE_UART_COMM_PROTOCOL):
    """Sigfox LPWAN transport (serial AT modem)."""

    protocol_name: ClassVar[str] = "sigfox"
    _TOKEN_ALIASES: ClassVar[frozenset[str]] = frozenset({"sigfox"})

    @classmethod
    def can_handle_uri(cls, uri: str) -> bool:
        """Claim ``sigfox://`` URIs."""
        return uri.startswith("sigfox://")
