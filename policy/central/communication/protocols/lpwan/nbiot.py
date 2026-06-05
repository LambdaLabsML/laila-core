"""NB-IoT (Narrowband IoT) communication transport.

NB-IoT is a cellular LPWAN bearer; once the modem attaches and a PDP
context is up the data path is IP, so this subclasses the generic
cellular transport with NB-IoT-specific tokens/URI.

- ``protocol_name`` ``"nbiot"`` (aliases ``nb-iot`` / ``nb1``)
- URI scheme ``nbiot://host:port``
"""

from __future__ import annotations

from typing import ClassVar

from ..cellular.cellular import _LAILA_IDENTIFIABLE_CELLULAR_COMM_PROTOCOL


class _LAILA_IDENTIFIABLE_NBIOT_COMM_PROTOCOL(_LAILA_IDENTIFIABLE_CELLULAR_COMM_PROTOCOL):
    """NB-IoT cellular LPWAN transport (IP data path)."""

    protocol_name: ClassVar[str] = "nbiot"
    _TOKEN_ALIASES: ClassVar[frozenset[str]] = frozenset({"nbiot", "nb-iot", "nb1"})

    @classmethod
    def can_handle_uri(cls, uri: str) -> bool:
        """Claim ``nbiot://`` URIs."""
        return uri.startswith("nbiot://")
