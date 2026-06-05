"""I2C communication transport.

Carries JSON-RPC over an I2C byte mailbox using the
:class:`_RegisterRPCProtocol` carrier and ``smbus2``. The local policy
is the I2C master; the peer device exposes an inbox/outbox register
region. Frames are length-prefixed and chunked to the 32-byte SMBus
block limit.

- ``protocol_name`` ``"i2c"``
- URI scheme ``i2c://<bus>/<addr>``

``smbus2`` is imported lazily; a missing library raises a clear error.
"""

from __future__ import annotations

from typing import Any, ClassVar

from pydantic import Field, PrivateAttr

from .._carriers.register import _RegisterRPCProtocol


class _LAILA_IDENTIFIABLE_I2C_COMM_PROTOCOL(_RegisterRPCProtocol):
    """I2C register-mailbox transport (master side)."""

    protocol_name: ClassVar[str] = "i2c"
    _TOKEN_ALIASES: ClassVar[frozenset[str]] = frozenset({"i2c"})

    bus_number: int = Field(default=1)
    address: int = Field(default=0x20)
    inbox_reg: int = Field(default=0x00)
    outbox_reg: int = Field(default=0x40)

    _bus: Any = PrivateAttr(default=None)

    @classmethod
    def can_handle_uri(cls, uri: str) -> bool:
        """Claim ``i2c://`` URIs."""
        return uri.startswith("i2c://")

    async def _open_bus(self) -> None:
        self._require_drivers(("smbus2",), "i2c")
        from smbus2 import SMBus  # noqa: PLC0415

        self._bus = SMBus(self.bus_number)

    async def _close_bus(self) -> None:
        if self._bus is not None:
            try:
                self._bus.close()
            except Exception:  # noqa: BLE001
                pass
            self._bus = None

    async def _deliver(self, data: bytes) -> None:
        payload = len(data).to_bytes(2, "big") + data
        # chunk to the 32-byte SMBus block limit
        for offset in range(0, len(payload), 30):
            chunk = list(payload[offset : offset + 30])
            self._bus.write_i2c_block_data(self.address, self.outbox_reg, chunk)

    async def _poll_inbound(self) -> bytes | None:
        header = self._bus.read_i2c_block_data(self.address, self.inbox_reg, 2)
        length = (header[0] << 8) | header[1]
        if length == 0:
            return None
        out = bytearray()
        remaining = length
        while remaining > 0:
            n = min(30, remaining)
            out += bytes(self._bus.read_i2c_block_data(self.address, self.inbox_reg + 2, n))
            remaining -= n
        # acknowledge by zeroing the length register
        self._bus.write_i2c_block_data(self.address, self.inbox_reg, [0, 0])
        return bytes(out[:length])
