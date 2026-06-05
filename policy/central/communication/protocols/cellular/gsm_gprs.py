"""GSM / GPRS (2G) communication transport.

2G data (GPRS/EDGE) as an IP carrier; a thin subclass of the generic
cellular transport with 2G-specific tokens/URI.

- ``protocol_name`` ``"gsm"`` (aliases ``gprs`` / ``2g`` / ``edge``)
- URI scheme ``gsm://host:port``
"""

from __future__ import annotations

from typing import ClassVar

from .cellular import _LAILA_IDENTIFIABLE_CELLULAR_COMM_PROTOCOL


class _LAILA_IDENTIFIABLE_GSM_COMM_PROTOCOL(_LAILA_IDENTIFIABLE_CELLULAR_COMM_PROTOCOL):
    """GSM/GPRS (2G) transport."""

    protocol_name: ClassVar[str] = "gsm"
    _TOKEN_ALIASES: ClassVar[frozenset[str]] = frozenset({"gsm", "gprs", "2g", "edge"})

    @classmethod
    def can_handle_uri(cls, uri: str) -> bool:
        """Claim ``gsm://`` URIs."""
        return uri.startswith("gsm://")
