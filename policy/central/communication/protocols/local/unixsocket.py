"""Unix domain socket communication transport.

Same-host inter-policy RPC over an :func:`asyncio.start_unix_server`
stream -- the fast, file-permission-secured local channel (no TCP port
exposed). Built on the reliable :class:`_StreamRPCProtocol` carrier.

- ``protocol_name`` ``"unix"`` (aliases ``uds`` / ``unixsocket``)
- URI scheme ``unix:///path/to/socket``
"""

from __future__ import annotations

import asyncio
import hashlib
import os
import tempfile
from typing import Any, ClassVar

from pydantic import Field, PrivateAttr

from .._carriers.stream import _StreamRPCProtocol
from .._carriers.uri import uri_path


class _LAILA_IDENTIFIABLE_UNIXSOCKET_COMM_PROTOCOL(_StreamRPCProtocol):
    """Unix-domain-socket peer-to-peer transport.

    Parameters
    ----------
    path : str, optional
        Filesystem path of the listening socket. When empty a stable
        path is derived under the system temp dir from this protocol's
        uuid, so :attr:`bound_path` is meaningful even with no config.
    """

    protocol_name: ClassVar[str] = "unix"
    _TOKEN_ALIASES: ClassVar[frozenset[str]] = frozenset({"unix", "uds", "unixsocket"})

    path: str = Field(default="")

    _bound_path: str | None = PrivateAttr(default=None)

    @property
    def bound_path(self) -> str:
        """Path the server is (or would be) listening on."""
        return self._bound_path or self._effective_path()

    def _effective_path(self) -> str:
        if self.path:
            return self.path
        digest = hashlib.sha1(str(self.uuid).encode("utf-8")).hexdigest()[:16]
        return os.path.join(tempfile.gettempdir(), f"laila_unix_{digest}.sock")

    @classmethod
    def matches_token(cls, token: str) -> bool:
        """Accept ``"unix"`` plus aliases."""
        return token.lower() in cls._TOKEN_ALIASES

    @classmethod
    def can_handle_uri(cls, uri: str) -> bool:
        """Claim ``unix://`` URIs."""
        return uri.startswith("unix://")

    async def _serve(self) -> Any:
        path = self._effective_path()
        if os.path.exists(path):
            os.unlink(path)
        server = await asyncio.start_unix_server(self._handle_inbound_stream, path=path)
        self._bound_path = path
        return server

    async def _open_connection(self, uri: str):
        path = uri_path(uri)
        return await asyncio.open_unix_connection(path)

    async def _close_server(self, server: Any) -> None:
        await super()._close_server(server)
        if self._bound_path and os.path.exists(self._bound_path):
            try:
                os.unlink(self._bound_path)
            except OSError:
                pass
        self._bound_path = None

    def connect_unix(self, path: str, secret: str) -> str:
        """Convenience wrapper building the ``unix://`` URI for :meth:`connect`."""
        return self.connect(f"unix://{path}", secret)
