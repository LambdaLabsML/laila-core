"""gRPC communication transport.

Carries JSON-RPC frames over a bidirectional gRPC stream via ``grpcio``.
Built on the point-to-point stream carrier; the driver is imported on
start and a missing library raises a clear capability error.

- ``protocol_name`` ``"grpc"``
- URI scheme ``grpc://host:port``
"""

from __future__ import annotations

import asyncio
from typing import Any, ClassVar

from pydantic import Field

from .._carriers.p2p import _P2PStreamRPCProtocol


class _LAILA_IDENTIFIABLE_GRPC_COMM_PROTOCOL(_P2PStreamRPCProtocol):
    """gRPC bidirectional-stream transport."""

    protocol_name: ClassVar[str] = "grpc"
    _TOKEN_ALIASES: ClassVar[frozenset[str]] = frozenset({"grpc"})

    target: str = Field(default="127.0.0.1:50051")

    @classmethod
    def can_handle_uri(cls, uri: str) -> bool:
        """Claim ``grpc://`` URIs."""
        return uri.startswith("grpc://")

    async def _open_stream(self) -> tuple[asyncio.StreamReader, Any]:
        self._require_drivers(("grpc",), "grpc")
        raise RuntimeError(
            "gRPC transport requires a generated bidi-streaming service stub; "
            "configure the gRPC channel/target before peering."
        )
