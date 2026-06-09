"""SPI communication transport.

Carries JSON-RPC over an SPI byte mailbox using the
:class:`_RegisterRPCProtocol` carrier and ``spidev``. The local policy
is the SPI master; the peer exposes a length-prefixed inbox/outbox.

- ``protocol_name`` ``"spi"``
- URI scheme ``spi://<bus>.<device>``

``spidev`` is imported lazily; a missing library raises a clear error.
"""

from __future__ import annotations

from typing import Any, ClassVar

from pydantic import Field, PrivateAttr

from .._carriers.register import _RegisterRPCProtocol


class _LAILA_IDENTIFIABLE_SPI_COMM_PROTOCOL(_RegisterRPCProtocol):
    """SPI register-mailbox transport (master side)."""

    protocol_name: ClassVar[str] = "spi"
    _TOKEN_ALIASES: ClassVar[frozenset[str]] = frozenset({"spi"})

    bus: int = Field(default=0)
    device: int = Field(default=0)
    max_speed_hz: int = Field(default=1_000_000)

    _spi: Any = PrivateAttr(default=None)

    @classmethod
    def can_handle_uri(cls, uri: str) -> bool:
        """Claim ``spi://`` URIs."""
        return uri.startswith("spi://")

    async def _open_bus(self) -> None:
        self._require_drivers(("spidev",), "spi")
        import spidev

        spi = spidev.SpiDev()
        spi.open(self.bus, self.device)
        spi.max_speed_hz = self.max_speed_hz
        self._spi = spi

    async def _close_bus(self) -> None:
        if self._spi is not None:
            try:
                self._spi.close()
            except Exception:
                pass
            self._spi = None

    async def _deliver(self, data: bytes) -> None:
        frame = len(data).to_bytes(2, "big") + data
        self._spi.xfer2(list(frame))

    async def _poll_inbound(self) -> bytes | None:
        header = bytes(self._spi.xfer2([0, 0]))
        length = (header[0] << 8) | header[1]
        if length == 0:
            return None
        return bytes(self._spi.xfer2([0] * length))
