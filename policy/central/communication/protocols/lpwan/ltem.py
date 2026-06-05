"""LTE-M (Cat-M1) communication transport.

LTE-M is a cellular LPWAN bearer; once attached the data path is IP, so
this subclasses the generic cellular transport with LTE-M-specific
tokens/URI.

- ``protocol_name`` ``"ltem"`` (aliases ``lte-m`` / ``cat-m1`` / ``catm1``)
- URI scheme ``ltem://host:port``
"""

from __future__ import annotations

from typing import ClassVar

from ..cellular.cellular import _LAILA_IDENTIFIABLE_CELLULAR_COMM_PROTOCOL


class _LAILA_IDENTIFIABLE_LTEM_COMM_PROTOCOL(_LAILA_IDENTIFIABLE_CELLULAR_COMM_PROTOCOL):
    """LTE-M (Cat-M1) cellular LPWAN transport (IP data path)."""

    protocol_name: ClassVar[str] = "ltem"
    _TOKEN_ALIASES: ClassVar[frozenset[str]] = frozenset(
        {"ltem", "lte-m", "cat-m1", "catm1"}
    )

    @classmethod
    def can_handle_uri(cls, uri: str) -> bool:
        """Claim ``ltem://`` URIs."""
        return uri.startswith("ltem://")
