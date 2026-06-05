"""5G NR communication transport.

5G New Radio data as an IP carrier; a thin subclass of the generic
cellular transport with 5G-specific tokens/URI.

- ``protocol_name`` ``"nr5g"`` (aliases ``5g`` / ``5g-nr`` / ``nr``)
- URI scheme ``nr5g://host:port`` (URI schemes cannot begin with a
  digit, so ``5g://`` is spelled ``nr5g://``)
"""

from __future__ import annotations

from typing import ClassVar

from .cellular import _LAILA_IDENTIFIABLE_CELLULAR_COMM_PROTOCOL


class _LAILA_IDENTIFIABLE_NR5G_COMM_PROTOCOL(_LAILA_IDENTIFIABLE_CELLULAR_COMM_PROTOCOL):
    """5G NR transport."""

    protocol_name: ClassVar[str] = "nr5g"
    _TOKEN_ALIASES: ClassVar[frozenset[str]] = frozenset({"nr5g", "5g", "5g-nr", "nr"})

    @classmethod
    def can_handle_uri(cls, uri: str) -> bool:
        """Claim ``nr5g://`` URIs."""
        return uri.startswith("nr5g://")
