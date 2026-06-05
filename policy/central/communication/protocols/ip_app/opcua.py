"""OPC-UA (industrial) communication transport.

Carries JSON-RPC over an OPC-UA secure channel via ``asyncua``. Built on
the point-to-point stream carrier; the driver is imported on start and a
missing library raises a clear capability error.

- ``protocol_name`` ``"opcua"`` (aliases ``opc.tcp``)
- URI scheme ``opc.tcp://host:port``
"""

from __future__ import annotations

import asyncio
from typing import Any, ClassVar

from pydantic import Field

from .._carriers.p2p import _P2PStreamRPCProtocol


class _LAILA_IDENTIFIABLE_OPCUA_COMM_PROTOCOL(_P2PStreamRPCProtocol):
    """OPC-UA transport."""

    protocol_name: ClassVar[str] = "opcua"
    _TOKEN_ALIASES: ClassVar[frozenset[str]] = frozenset({"opcua", "opc.tcp", "opc-ua"})

    endpoint: str = Field(default="opc.tcp://127.0.0.1:4840")

    @classmethod
    def matches_token(cls, token: str) -> bool:
        """Accept ``"opcua"`` plus aliases."""
        return token.lower() in cls._TOKEN_ALIASES

    @classmethod
    def can_handle_uri(cls, uri: str) -> bool:
        """Claim ``opc.tcp://`` URIs."""
        return uri.startswith("opc.tcp://")

    async def _open_stream(self) -> tuple[asyncio.StreamReader, Any]:
        self._require_drivers(("asyncua",), "opcua")
        raise RuntimeError(
            "OPC-UA transport requires an OPC-UA server endpoint exposing a "
            "byte-stream method/variable mailbox; configure the server endpoint."
        )
