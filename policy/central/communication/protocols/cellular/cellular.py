"""Cellular (generic WWAN) communication transport.

Cellular is an IP carrier: once the modem has a data session (PDP/PDN
context up), the data path is plain TCP, so this transport subclasses
the raw-TCP transport and adds modem/APN identity plus its own
token/URI. Bringing the data session up is a modem/OS concern
(``ModemManager`` / AT commands); ``apn`` / ``modem_device`` are
recorded for diagnostics and future link bring-up.

- ``protocol_name`` ``"cellular"`` (aliases ``wwan`` / ``modem``)
- URI scheme ``cellular://host:port``

Specific generations (2G/4G/5G) subclass this with their own tokens.
"""

from __future__ import annotations

from typing import ClassVar

from pydantic import Field

from ..ip_app.tcp import _LAILA_IDENTIFIABLE_TCP_COMM_PROTOCOL


class _LAILA_IDENTIFIABLE_CELLULAR_COMM_PROTOCOL(_LAILA_IDENTIFIABLE_TCP_COMM_PROTOCOL):
    """Generic cellular WWAN transport (TCP data path over a modem)."""

    protocol_name: ClassVar[str] = "cellular"
    _TOKEN_ALIASES: ClassVar[frozenset[str]] = frozenset({"cellular", "wwan", "modem"})

    #: Access Point Name for the data session (carrier-specific).
    apn: str | None = Field(default=None)
    #: Modem control device (e.g. ``"/dev/cdc-wdm0"``). Informational.
    modem_device: str | None = Field(default=None)

    @classmethod
    def can_handle_uri(cls, uri: str) -> bool:
        """Claim ``cellular://`` URIs."""
        return uri.startswith("cellular://")
