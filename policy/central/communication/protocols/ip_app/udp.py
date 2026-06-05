"""UDP communication transport.

Connectionless datagram transport built on the
:class:`_DatagramRPCProtocol` carrier, which layers fragmentation,
ack/retry and dedup on top of raw UDP so the lossy link still carries
reliable JSON-RPC.

- ``protocol_name`` ``"udp"`` (aliases ``udp4`` / ``udp6``)
- URI scheme ``udp://host:port``
"""

from __future__ import annotations

import asyncio
from typing import Any, ClassVar

from pydantic import Field, PrivateAttr

from .._carriers.datagram import _DatagramEndpoint, _DatagramRPCProtocol
from .._carriers.uri import split_host_port


class _LAILA_IDENTIFIABLE_UDP_COMM_PROTOCOL(_DatagramRPCProtocol):
    """UDP peer-to-peer transport with a reliability layer.

    Parameters
    ----------
    host : str, default ``"0.0.0.0"``
        Local bind address.
    port : int, default ``0``
        Local UDP port (``0`` lets the OS choose; see :attr:`bound_port`).
    """

    protocol_name: ClassVar[str] = "udp"
    _TOKEN_ALIASES: ClassVar[frozenset[str]] = frozenset({"udp", "udp4", "udp6"})

    host: str = Field(default="0.0.0.0")
    port: int = Field(default=0)

    _bound_port: int | None = PrivateAttr(default=None)

    @property
    def bound_port(self) -> int:
        """OS-assigned local port after :meth:`start`."""
        return self._bound_port if self._bound_port is not None else self.port

    @classmethod
    def matches_token(cls, token: str) -> bool:
        """Accept ``"udp"`` plus aliases."""
        return token.lower() in cls._TOKEN_ALIASES

    @classmethod
    def can_handle_uri(cls, uri: str) -> bool:
        """Claim ``udp://`` URIs."""
        return uri.startswith("udp://")

    async def _create_datagram_endpoint(self) -> tuple[Any, Any]:
        loop = asyncio.get_running_loop()
        transport, proto = await loop.create_datagram_endpoint(
            lambda: _DatagramEndpoint(self._feed_packet),
            local_addr=(self.host, self.port),
        )
        self._bound_port = transport.get_extra_info("sockname")[1]
        return transport, proto

    async def _resolve_peer_addr(self, uri: str) -> Any:
        host, port = split_host_port(uri)
        return (host, port)

    def connect_udp(self, host: str, port: int, secret: str) -> str:
        """Convenience wrapper building the ``udp://`` URI for :meth:`connect`."""
        return self.connect(f"udp://{host}:{port}", secret)
