"""LTE (4G) communication transport.

4G LTE data as an IP carrier; a thin subclass of the generic cellular
transport with LTE-specific tokens/URI.

- ``protocol_name`` ``"lte"`` (aliases ``4g``)
- URI scheme ``lte://host:port``
"""

from __future__ import annotations

from typing import ClassVar

from .cellular import _LAILA_IDENTIFIABLE_CELLULAR_COMM_PROTOCOL


class _LAILA_IDENTIFIABLE_LTE_COMM_PROTOCOL(_LAILA_IDENTIFIABLE_CELLULAR_COMM_PROTOCOL):
    """LTE (4G) transport."""

    protocol_name: ClassVar[str] = "lte"
    _TOKEN_ALIASES: ClassVar[frozenset[str]] = frozenset({"lte", "4g"})

    @classmethod
    def can_handle_uri(cls, uri: str) -> bool:
        """Claim ``lte://`` URIs."""
        return uri.startswith("lte://")
