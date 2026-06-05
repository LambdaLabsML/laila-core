"""Wi-Fi Direct (P2P) communication transport.

Wi-Fi Direct forms a direct IP link between two peers with no access
point; once the P2P group is up the data path is plain TCP, so this
subclasses the raw-TCP transport. Group formation is an OS concern
(``wpa_supplicant`` P2P); its own token/URI distinguish it.

- ``protocol_name`` ``"wifi-direct"`` (aliases ``wifidirect`` / ``wfd`` / ``p2p``)
- URI scheme ``wifidirect://host:port``
"""

from __future__ import annotations

from typing import ClassVar

from ..ip_app.tcp import _LAILA_IDENTIFIABLE_TCP_COMM_PROTOCOL


class _LAILA_IDENTIFIABLE_WIFIDIRECT_COMM_PROTOCOL(_LAILA_IDENTIFIABLE_TCP_COMM_PROTOCOL):
    """Wi-Fi Direct (P2P) transport (TCP data path over a P2P group)."""

    protocol_name: ClassVar[str] = "wifi-direct"
    _TOKEN_ALIASES: ClassVar[frozenset[str]] = frozenset(
        {"wifi-direct", "wifidirect", "wfd", "p2p"}
    )

    @classmethod
    def can_handle_uri(cls, uri: str) -> bool:
        """Claim ``wifidirect://`` URIs."""
        return uri.startswith("wifidirect://")
