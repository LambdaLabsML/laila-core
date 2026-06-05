"""Z-Wave communication transport (datagram radio).

Carries fragmented JSON-RPC over a Z-Wave network via a Z-Wave JS server
(``zwave-js-server-python``). Built on the lazy datagram carrier.

- ``protocol_name`` ``"zwave"`` (aliases ``z-wave``)
- URI scheme ``zwave://<node>``
"""

from __future__ import annotations

from typing import ClassVar

from .._carriers.lazy import _LazyDatagramRPCProtocol


class _LAILA_IDENTIFIABLE_ZWAVE_COMM_PROTOCOL(_LazyDatagramRPCProtocol):
    """Z-Wave transport."""

    protocol_name: ClassVar[str] = "zwave"
    _TOKEN_ALIASES: ClassVar[frozenset[str]] = frozenset({"zwave", "z-wave"})
    _DRIVER_MODULES: ClassVar[tuple] = ("zwave_js_server",)
    _DRIVER_EXTRA: ClassVar[str] = "zwave"

    @classmethod
    def matches_token(cls, token: str) -> bool:
        """Accept ``"zwave"`` / ``"z-wave"``."""
        return token.lower() in cls._TOKEN_ALIASES

    @classmethod
    def can_handle_uri(cls, uri: str) -> bool:
        """Claim ``zwave://`` URIs."""
        return uri.startswith("zwave://")
