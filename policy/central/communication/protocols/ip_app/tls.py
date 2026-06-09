"""TLS communication transport (TCP + SSL).

Subclasses the raw-TCP transport and wraps both the listener and the
outbound connection in an :class:`ssl.SSLContext` built from the
configured certificate material. Working but **config-required**: a
server needs ``certfile`` (+ ``keyfile``); there is no auto-generated
certificate (that would pull in a crypto dependency), so a missing cert
raises a clear error at :meth:`start`.

- ``protocol_name`` ``"tls"`` (aliases ``tcps`` / ``ssl`` / ``tls/tcp``)
- URI scheme ``tls://host:port`` (also ``tcps://``)
"""

from __future__ import annotations

import asyncio
import ssl
from typing import Any, ClassVar

from pydantic import Field

from .._carriers.uri import split_host_port
from .tcp import _LAILA_IDENTIFIABLE_TCP_COMM_PROTOCOL


class _LAILA_IDENTIFIABLE_TLS_COMM_PROTOCOL(_LAILA_IDENTIFIABLE_TCP_COMM_PROTOCOL):
    """TLS-secured peer-to-peer transport.

    Parameters
    ----------
    certfile : str, optional
        PEM certificate for the listener (required to serve).
    keyfile : str, optional
        PEM private key for ``certfile``.
    cafile : str, optional
        CA bundle used to verify the peer. When omitted on the client
        side, verification is disabled (``check_hostname=False``,
        ``verify_mode=CERT_NONE``) so self-signed test certs work.
    server_hostname : str, optional
        Name presented/verified during the client TLS handshake.
    """

    protocol_name: ClassVar[str] = "tls"
    _TOKEN_ALIASES: ClassVar[frozenset[str]] = frozenset({"tls", "tcps", "ssl", "tls/tcp"})

    certfile: str | None = Field(default=None)
    keyfile: str | None = Field(default=None)
    cafile: str | None = Field(default=None)
    server_hostname: str | None = Field(default=None)

    @classmethod
    def can_handle_uri(cls, uri: str) -> bool:
        """Claim ``tls://`` and ``tcps://`` URIs."""
        return uri.startswith("tls://") or uri.startswith("tcps://")

    def _server_ssl_context(self) -> ssl.SSLContext:
        if not self.certfile:
            raise RuntimeError(
                "TLS transport requires `certfile` (and `keyfile`) to serve. "
                "Set them on the protocol or via laila.args."
            )
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ctx.load_cert_chain(certfile=self.certfile, keyfile=self.keyfile)
        return ctx

    def _client_ssl_context(self) -> ssl.SSLContext:
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        if self.cafile:
            ctx.load_verify_locations(cafile=self.cafile)
        else:
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
        return ctx

    async def _serve(self) -> Any:
        # No certificate -> client-only endpoint (can peer out, cannot
        # accept inbound). Avoids forcing a cert on pure clients.
        if not self.certfile:
            return None
        server = await asyncio.start_server(
            self._handle_inbound_stream,
            self.host,
            self.port,
            ssl=self._server_ssl_context(),
        )
        self._bound_port = server.sockets[0].getsockname()[1]
        return server

    async def _open_connection(self, uri: str):
        host, port = split_host_port(uri)
        return await asyncio.open_connection(
            host,
            port,
            ssl=self._client_ssl_context(),
            server_hostname=self.server_hostname or host,
        )

    def connect_tls(self, host: str, port: int, secret: str) -> str:
        """Convenience wrapper building the ``tls://`` URI for :meth:`connect`."""
        return self.connect(f"tls://{host}:{port}", secret)
