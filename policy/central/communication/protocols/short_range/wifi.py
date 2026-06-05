"""Wi-Fi (station / AP) communication transport.

Wi-Fi is an IP carrier: once associated, the data path is plain TCP, so
this transport subclasses the raw-TCP transport and adds Wi-Fi link
identity (``ssid`` / ``mode``) plus its own token/URI. Association to an
AP (or running as an AP) is an OS concern (``nmcli`` / ``wpa_supplicant``
/ ``hostapd``); those fields are recorded for diagnostics and future
link bring-up.

- ``protocol_name`` ``"wifi"`` (aliases ``wlan`` / ``sta`` / ``ap``)
- URI scheme ``wifi://host:port``
"""

from __future__ import annotations

from typing import ClassVar

from pydantic import Field

from ..ip_app.tcp import _LAILA_IDENTIFIABLE_TCP_COMM_PROTOCOL


class _LAILA_IDENTIFIABLE_WIFI_COMM_PROTOCOL(_LAILA_IDENTIFIABLE_TCP_COMM_PROTOCOL):
    """Wi-Fi station/AP transport (TCP data path over a wireless NIC)."""

    protocol_name: ClassVar[str] = "wifi"
    _TOKEN_ALIASES: ClassVar[frozenset[str]] = frozenset({"wifi", "wlan", "sta", "ap"})

    #: Wi-Fi network name to associate with (station mode) or advertise (AP mode).
    ssid: str | None = Field(default=None)
    #: ``"station"`` or ``"ap"``. Informational for OS-level bring-up.
    mode: str = Field(default="station")

    @classmethod
    def can_handle_uri(cls, uri: str) -> bool:
        """Claim ``wifi://`` URIs."""
        return uri.startswith("wifi://")

    def connect_wifi(self, host: str, port: int, secret: str) -> str:
        """Convenience wrapper building the ``wifi://`` URI."""
        return self.connect(f"wifi://{host}:{port}", secret)
