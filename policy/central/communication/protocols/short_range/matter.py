"""Matter (over Thread / Wi-Fi) communication transport.

Matter runs over IPv6 (carried by Thread or Wi-Fi), so once the device
is commissioned onto the fabric the data path is IP. This transport
subclasses the raw-TCP transport with Matter-specific tokens/URI;
fabric commissioning is handled out of band.

- ``protocol_name`` ``"matter"`` (aliases ``chip``)
- URI scheme ``matter://host:port``
"""

from __future__ import annotations

from typing import ClassVar

from ..ip_app.tcp import _LAILA_IDENTIFIABLE_TCP_COMM_PROTOCOL


class _LAILA_IDENTIFIABLE_MATTER_COMM_PROTOCOL(_LAILA_IDENTIFIABLE_TCP_COMM_PROTOCOL):
    """Matter transport (IP data path over Thread/Wi-Fi)."""

    protocol_name: ClassVar[str] = "matter"
    _TOKEN_ALIASES: ClassVar[frozenset[str]] = frozenset({"matter", "chip"})

    @classmethod
    def can_handle_uri(cls, uri: str) -> bool:
        """Claim ``matter://`` URIs."""
        return uri.startswith("matter://")
