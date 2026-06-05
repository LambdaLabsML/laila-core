"""6LoWPAN communication transport.

6LoWPAN carries IPv6 over IEEE 802.15.4, so once the ``lowpan``/``wpan``
interface is up the data path is plain IP/UDP. This subclasses the UDP
transport with 6LoWPAN-specific tokens/URI; interface bring-up
(``mac802154_hwsim`` or real radio + ``ip link``) is an OS concern.

- ``protocol_name`` ``"6lowpan"`` (aliases ``sixlowpan``)
- URI scheme ``sixlowpan://[ipv6]:port``
"""

from __future__ import annotations

from typing import ClassVar

from ..ip_app.udp import _LAILA_IDENTIFIABLE_UDP_COMM_PROTOCOL


class _LAILA_IDENTIFIABLE_SIXLOWPAN_COMM_PROTOCOL(_LAILA_IDENTIFIABLE_UDP_COMM_PROTOCOL):
    """6LoWPAN transport (IPv6/UDP over 802.15.4)."""

    protocol_name: ClassVar[str] = "6lowpan"
    _TOKEN_ALIASES: ClassVar[frozenset[str]] = frozenset({"6lowpan", "sixlowpan"})

    @classmethod
    def can_handle_uri(cls, uri: str) -> bool:
        """Claim ``sixlowpan://`` URIs."""
        return uri.startswith("sixlowpan://")
