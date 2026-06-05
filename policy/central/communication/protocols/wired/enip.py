"""EtherNet/IP (CIP) communication transport.

Carries JSON-RPC over an EtherNet/IP tag mailbox using the
:class:`_RegisterRPCProtocol` carrier and ``pycomm3``. The local policy
reads/writes string tags on the peer PLC that serve as the inbox/outbox.

- ``protocol_name`` ``"ethernet-ip"`` (aliases ``enip`` / ``ethernetip``)
- URI scheme ``enip://host``

``pycomm3`` is imported lazily; a missing library raises a clear error.
"""

from __future__ import annotations

from typing import Any, ClassVar

from pydantic import Field, PrivateAttr

from .._carriers.register import _RegisterRPCProtocol
from .._carriers.uri import uri_authority


class _LAILA_IDENTIFIABLE_ENIP_COMM_PROTOCOL(_RegisterRPCProtocol):
    """EtherNet/IP tag-mailbox transport."""

    protocol_name: ClassVar[str] = "ethernet-ip"
    _TOKEN_ALIASES: ClassVar[frozenset[str]] = frozenset(
        {"ethernet-ip", "enip", "ethernetip"}
    )

    host: str = Field(default="127.0.0.1")
    inbox_tag: str = Field(default="LAILA_IN")
    outbox_tag: str = Field(default="LAILA_OUT")

    _plc: Any = PrivateAttr(default=None)

    @classmethod
    def matches_token(cls, token: str) -> bool:
        """Accept ``"ethernet-ip"`` plus aliases."""
        return token.lower() in cls._TOKEN_ALIASES

    @classmethod
    def can_handle_uri(cls, uri: str) -> bool:
        """Claim ``enip://`` URIs."""
        return uri.startswith("enip://")

    async def _open_bus(self) -> None:
        self._require_drivers(("pycomm3",), "enip")
        from pycomm3 import CIPDriver  # noqa: PLC0415

        self._plc = CIPDriver(self.host)
        self._plc.open()

    async def _close_bus(self) -> None:
        if self._plc is not None:
            try:
                self._plc.close()
            except Exception:  # noqa: BLE001
                pass
            self._plc = None

    async def _deliver(self, data: bytes) -> None:
        # store as a base64 string tag (PLC string tags are ASCII)
        import base64  # noqa: PLC0415

        self._plc.write((self.outbox_tag, base64.b64encode(data).decode("ascii")))

    async def _poll_inbound(self) -> bytes | None:
        import base64  # noqa: PLC0415

        result = self._plc.read(self.inbox_tag)
        value = getattr(result, "value", None)
        if not value:
            return None
        self._plc.write((self.inbox_tag, ""))
        return base64.b64decode(value)
