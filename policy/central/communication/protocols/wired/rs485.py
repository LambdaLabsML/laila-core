"""RS-485 communication transport.

RS-485 is a half-duplex, multidrop serial bus. At the byte level it is
the same UART stream, so this subclasses the UART transport and enables
pyserial's RS-485 direction-control (DE/RE toggling) when the driver
supports it.

- ``protocol_name`` ``"rs485"`` (aliases ``rs-485``)
- URI scheme ``rs485:///dev/ttyUSB0``
"""

from __future__ import annotations

import logging
from typing import Any, ClassVar

from pydantic import Field

from .uart import _LAILA_IDENTIFIABLE_UART_COMM_PROTOCOL

log = logging.getLogger(__name__)


class _LAILA_IDENTIFIABLE_RS485_COMM_PROTOCOL(_LAILA_IDENTIFIABLE_UART_COMM_PROTOCOL):
    """RS-485 half-duplex multidrop serial transport.

    Parameters
    ----------
    rs485_mode : bool, default ``True``
        Enable pyserial's RS-485 direction control on the port.
    """

    protocol_name: ClassVar[str] = "rs485"
    _TOKEN_ALIASES: ClassVar[frozenset[str]] = frozenset({"rs485", "rs-485"})

    rs485_mode: bool = Field(default=True)

    @classmethod
    def can_handle_uri(cls, uri: str) -> bool:
        """Claim ``rs485://`` URIs."""
        return uri.startswith("rs485://")

    def _configure_serial(self, ser: Any) -> None:
        if not self.rs485_mode:
            return
        try:
            import serial.rs485  # noqa: PLC0415

            ser.rs485_mode = serial.rs485.RS485Settings()
        except Exception:  # noqa: BLE001 - not all drivers support it
            log.debug("RS-485 direction control unavailable on %s", self.port)
