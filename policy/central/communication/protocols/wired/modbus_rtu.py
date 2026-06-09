"""Modbus-RTU (over RS-485) communication transport.

Same register-mailbox approach as Modbus-TCP but over an RS-485 serial
line via ``pymodbus``'s async serial client.

- ``protocol_name`` ``"modbus-rtu"`` (aliases ``modbusrtu``)
- URI scheme ``modbusrtu:///dev/ttyUSB0``

``pymodbus`` is imported lazily; a missing library raises a clear error.
"""

from __future__ import annotations

from typing import Any, ClassVar

from pydantic import Field, PrivateAttr

from ..ip_app.modbus_tcp import _LAILA_IDENTIFIABLE_MODBUS_TCP_COMM_PROTOCOL


class _LAILA_IDENTIFIABLE_MODBUS_RTU_COMM_PROTOCOL(_LAILA_IDENTIFIABLE_MODBUS_TCP_COMM_PROTOCOL):
    """Modbus-RTU register-mailbox transport over RS-485."""

    protocol_name: ClassVar[str] = "modbus-rtu"
    _TOKEN_ALIASES: ClassVar[frozenset[str]] = frozenset({"modbus-rtu", "modbusrtu"})

    port: str = Field(default="")  # serial device, e.g. /dev/ttyUSB0
    baudrate: int = Field(default=9600)

    _client: Any = PrivateAttr(default=None)

    @classmethod
    def can_handle_uri(cls, uri: str) -> bool:
        """Claim ``modbusrtu://`` URIs."""
        return uri.startswith("modbusrtu://")

    async def _open_bus(self) -> None:
        self._require_drivers(("pymodbus",), "modbus")
        if not self.port:
            raise RuntimeError(
                f"{type(self).__name__} requires a serial `port` (e.g. '/dev/ttyUSB0')."
            )
        from pymodbus.client import AsyncModbusSerialClient

        self._client = AsyncModbusSerialClient(port=self.port, baudrate=self.baudrate)
        await self._client.connect()
