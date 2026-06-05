"""1-Wire (Dallas) communication transport (datagram).

1-Wire is a master/slave sensor bus (Linux ``w1`` sysfs); it is not a
symmetric peer link, so this lazy datagram transport raises a clear
hardware-requirement error on start.

- ``protocol_name`` ``"1-wire"`` (aliases ``onewire`` / ``w1``)
- URI scheme ``onewire://<id>``
"""

from __future__ import annotations

from typing import ClassVar

from .._carriers.lazy import _LazyDatagramRPCProtocol


class _LAILA_IDENTIFIABLE_ONEWIRE_COMM_PROTOCOL(_LazyDatagramRPCProtocol):
    """1-Wire sensor-bus transport."""

    protocol_name: ClassVar[str] = "1-wire"
    _TOKEN_ALIASES: ClassVar[frozenset[str]] = frozenset({"1-wire", "onewire", "w1"})

    @classmethod
    def matches_token(cls, token: str) -> bool:
        """Accept ``"1-wire"`` / ``"onewire"`` / ``"w1"``."""
        return token.lower() in cls._TOKEN_ALIASES

    @classmethod
    def can_handle_uri(cls, uri: str) -> bool:
        """Claim ``onewire://`` URIs."""
        return uri.startswith("onewire://")
