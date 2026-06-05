"""IrDA / IR communication transport (datagram radio).

IrDA support has been removed from modern Linux kernels and has no
common host-side Python driver, so this lazy datagram transport raises a
clear hardware-requirement error on start.

- ``protocol_name`` ``"irda"`` (aliases ``ir``)
- URI scheme ``irda://<addr>``
"""

from __future__ import annotations

from typing import ClassVar

from .._carriers.lazy import _LazyDatagramRPCProtocol


class _LAILA_IDENTIFIABLE_IRDA_COMM_PROTOCOL(_LazyDatagramRPCProtocol):
    """IrDA / IR transport."""

    protocol_name: ClassVar[str] = "irda"
    _TOKEN_ALIASES: ClassVar[frozenset[str]] = frozenset({"irda", "ir"})

    @classmethod
    def matches_token(cls, token: str) -> bool:
        """Accept ``"irda"`` / ``"ir"``."""
        return token.lower() in cls._TOKEN_ALIASES

    @classmethod
    def can_handle_uri(cls, uri: str) -> bool:
        """Claim ``irda://`` URIs."""
        return uri.startswith("irda://")
