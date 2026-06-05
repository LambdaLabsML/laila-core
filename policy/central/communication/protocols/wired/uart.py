"""UART / serial communication transport.

Point-to-point framed JSON-RPC over an asynchronous serial line, built
on the :class:`_P2PStreamRPCProtocol` carrier. Uses ``pyserial`` to open
the port and the standard asyncio pipe trick
(``connect_read_pipe`` / ``connect_write_pipe`` on the serial fd) so the
blocking serial device participates as an asyncio stream -- no
``pyserial-asyncio`` dependency required.

- ``protocol_name`` ``"uart"`` (aliases ``serial``)
- URI scheme ``serial:///dev/ttyUSB0`` (informational; the port comes
  from the ``port`` field)

``pyserial`` is imported lazily inside :meth:`_open_stream`; a missing
library or device raises a clear, actionable error.
"""

from __future__ import annotations

import asyncio
import os
from typing import Any, ClassVar

from pydantic import Field, PrivateAttr

from .._carriers.p2p import _P2PStreamRPCProtocol

_INSTALL_HINT = (
    "The serial transport requires pyserial. Install it with "
    "`pip install laila-core[serial]` (or `pip install pyserial`)."
)


class _LAILA_IDENTIFIABLE_UART_COMM_PROTOCOL(_P2PStreamRPCProtocol):
    """Serial-line transport (UART).

    Parameters
    ----------
    port : str
        Serial device path (e.g. ``"/dev/ttyUSB0"``, ``"COM3"``).
    baudrate : int, default ``115200``
        Line speed.
    """

    protocol_name: ClassVar[str] = "uart"
    _TOKEN_ALIASES: ClassVar[frozenset[str]] = frozenset({"uart", "serial"})

    port: str = Field(default="")
    baudrate: int = Field(default=115200)

    _serial: Any = PrivateAttr(default=None)

    @classmethod
    def matches_token(cls, token: str) -> bool:
        """Accept ``"uart"`` / ``"serial"``."""
        return token.lower() in cls._TOKEN_ALIASES

    @classmethod
    def can_handle_uri(cls, uri: str) -> bool:
        """Claim ``serial://`` and ``uart://`` URIs."""
        return uri.startswith("serial://") or uri.startswith("uart://")

    def _make_serial(self):
        try:
            import serial  # noqa: PLC0415
        except ImportError as exc:  # pragma: no cover - exercised when dep absent
            raise RuntimeError(_INSTALL_HINT) from exc
        if not self.port:
            raise RuntimeError(
                f"{type(self).__name__} requires a `port` (e.g. '/dev/ttyUSB0')."
            )
        return serial.Serial(self.port, self.baudrate, timeout=0)

    async def _open_stream(self) -> tuple[asyncio.StreamReader, Any]:
        ser = self._make_serial()
        self._configure_serial(ser)
        self._serial = ser
        fd = ser.fileno()
        os.set_blocking(fd, False)
        loop = asyncio.get_running_loop()

        reader = asyncio.StreamReader()
        await loop.connect_read_pipe(
            lambda: asyncio.StreamReaderProtocol(reader),
            os.fdopen(fd, "rb", buffering=0, closefd=False),
        )
        w_transport, w_proto = await loop.connect_write_pipe(
            asyncio.streams.FlowControlMixin,
            os.fdopen(os.dup(fd), "wb", buffering=0),
        )
        writer = asyncio.StreamWriter(w_transport, w_proto, reader, loop)
        return reader, writer

    def _configure_serial(self, ser: Any) -> None:
        """Hook for subclasses (RS-485 direction control, etc.)."""
        return None

    async def _close_stream(self) -> None:
        await super()._close_stream()
        if self._serial is not None:
            try:
                self._serial.close()
            except Exception:  # noqa: BLE001
                pass
            self._serial = None
