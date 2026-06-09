"""Raw TCP communication transport.

A direct, framed TCP transport (no WebSocket/HTTP upgrade) built on the
reliable :class:`_StreamRPCProtocol` carrier. Lighter than the
WebSocket-based ``tcpip`` transport and the natural choice for embedded
peers that just want a socket.

- ``protocol_name`` ``"tcp"`` (aliases ``tcp4`` / ``tcp6`` / ``raw-tcp``)
- URI scheme ``tcp://host:port``
"""

from __future__ import annotations

import asyncio
from typing import Any, ClassVar

from pydantic import Field, PrivateAttr

from .._carriers.stream import _StreamRPCProtocol
from .._carriers.uri import split_host_port


class _LAILA_IDENTIFIABLE_TCP_COMM_PROTOCOL(_StreamRPCProtocol):
    """Raw-TCP peer-to-peer transport (framed JSON-RPC over a TCP socket).

    Parameters
    ----------
    host : str, default ``"0.0.0.0"``
        Bind address for the listener.
    port : int, default ``0``
        TCP port (``0`` lets the OS choose; see :attr:`bound_port`).
    """

    protocol_name: ClassVar[str] = "tcp"
    _TOKEN_ALIASES: ClassVar[frozenset[str]] = frozenset(
        {"tcp", "tcp4", "tcp6", "raw-tcp", "rawtcp"}
    )

    host: str = Field(default="0.0.0.0")
    port: int = Field(default=0)

    _bound_port: int | None = PrivateAttr(default=None)

    @property
    def bound_port(self) -> int:
        """OS-assigned port after :meth:`start` (falls back to :attr:`port`)."""
        return self._bound_port if self._bound_port is not None else self.port

    @classmethod
    def matches_token(cls, token: str) -> bool:
        """Accept ``"tcp"`` plus aliases."""
        return token.lower() in cls._TOKEN_ALIASES

    @classmethod
    def can_handle_uri(cls, uri: str) -> bool:
        """Claim ``tcp://`` URIs."""
        return uri.startswith("tcp://")

    def _on_stream_ready(self, writer) -> None:
        """Disable Nagle (TCP_NODELAY) so small RPC frames go out immediately."""
        import socket as _socket

        sock = writer.get_extra_info("socket")
        if sock is not None:
            try:
                sock.setsockopt(_socket.IPPROTO_TCP, _socket.TCP_NODELAY, 1)
            except (OSError, AttributeError):
                pass

    async def _serve(self) -> Any:
        server = await asyncio.start_server(self._handle_inbound_stream, self.host, self.port)
        self._bound_port = server.sockets[0].getsockname()[1]
        return server

    async def _open_connection(self, uri: str):
        host, port = split_host_port(uri)
        return await asyncio.open_connection(host, port)

    def connect_tcp(self, host: str, port: int, secret: str) -> str:
        """Convenience wrapper building the ``tcp://`` URI for :meth:`connect`."""
        return self.connect(f"tcp://{host}:{port}", secret)
