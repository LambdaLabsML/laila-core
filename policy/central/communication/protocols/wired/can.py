"""CAN / CAN-FD communication transport.

CAN is a multidrop bus with tiny frames (8 bytes classic, up to 64 for
CAN-FD), so JSON-RPC messages must be segmented. This transport uses
ISO-TP (``can-isotp``) over ``python-can`` to expose a reliable,
segmented stream and carries RPC on top of the point-to-point stream
carrier.

- ``protocol_name`` ``"can"`` (aliases ``canfd`` / ``can-fd``)
- URI scheme ``can://<channel>``

``python-can`` / ``can-isotp`` are imported lazily; a missing library
raises a clear, actionable error. End-to-end verification uses a
SocketCAN virtual interface (``vcan0``).
"""

from __future__ import annotations

import asyncio
from typing import Any, ClassVar

from pydantic import Field, PrivateAttr

from .._carriers.p2p import _P2PStreamRPCProtocol

_INSTALL_HINT = (
    "The CAN transport requires python-can and can-isotp. Install them with "
    "`pip install laila-core[can]` (or `pip install python-can can-isotp`), "
    "and bring up a CAN interface (e.g. `sudo modprobe vcan && "
    "sudo ip link add dev vcan0 type vcan && sudo ip link set up vcan0`)."
)


class _LAILA_IDENTIFIABLE_CAN_COMM_PROTOCOL(_P2PStreamRPCProtocol):
    """CAN / CAN-FD transport (ISO-TP segmented stream over SocketCAN).

    Parameters
    ----------
    channel : str, default ``"vcan0"``
        SocketCAN interface name.
    bustype : str, default ``"socketcan"``
        ``python-can`` bus backend.
    tx_id, rx_id : int
        ISO-TP arbitration ids for this endpoint's transmit / receive.
    fd : bool, default ``False``
        Use CAN-FD (64-byte frames).
    """

    protocol_name: ClassVar[str] = "can"
    _TOKEN_ALIASES: ClassVar[frozenset[str]] = frozenset({"can", "canfd", "can-fd"})

    channel: str = Field(default="vcan0")
    bustype: str = Field(default="socketcan")
    tx_id: int = Field(default=0x123)
    rx_id: int = Field(default=0x456)
    fd: bool = Field(default=False)

    _bus: Any = PrivateAttr(default=None)
    _isotp_sock: Any = PrivateAttr(default=None)

    @classmethod
    def matches_token(cls, token: str) -> bool:
        """Accept ``"can"`` plus CAN-FD aliases."""
        return token.lower() in cls._TOKEN_ALIASES

    @classmethod
    def can_handle_uri(cls, uri: str) -> bool:
        """Claim ``can://`` URIs."""
        return uri.startswith("can://")

    async def _open_stream(self) -> tuple[asyncio.StreamReader, Any]:
        try:
            import isotp  # noqa: PLC0415
        except ImportError as exc:  # pragma: no cover - exercised when dep absent
            raise RuntimeError(_INSTALL_HINT) from exc

        # ISO-TP socket gives a reliable, segmented byte stream over CAN.
        sock = isotp.socket()
        sock.set_fc_opts(stmin=5, bs=10)
        sock.bind(self.channel, isotp.Address(rxid=self.rx_id, txid=self.tx_id))
        self._isotp_sock = sock

        loop = asyncio.get_running_loop()
        fileno = sock.fileno()
        import os  # noqa: PLC0415

        os.set_blocking(fileno, False)
        reader = asyncio.StreamReader()
        await loop.connect_read_pipe(
            lambda: asyncio.StreamReaderProtocol(reader),
            os.fdopen(fileno, "rb", buffering=0, closefd=False),
        )
        w_transport, w_proto = await loop.connect_write_pipe(
            asyncio.streams.FlowControlMixin,
            os.fdopen(os.dup(fileno), "wb", buffering=0),
        )
        writer = asyncio.StreamWriter(w_transport, w_proto, reader, loop)
        return reader, writer

    async def _close_stream(self) -> None:
        await super()._close_stream()
        if self._isotp_sock is not None:
            try:
                self._isotp_sock.close()
            except Exception:  # noqa: BLE001
                pass
            self._isotp_sock = None
