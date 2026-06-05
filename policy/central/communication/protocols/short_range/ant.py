"""ANT / ANT+ communication transport (datagram radio).

Carries fragmented JSON-RPC over an ANT USB stick via ``openant``. Built
on the lazy datagram carrier.

- ``protocol_name`` ``"ant"`` (aliases ``ant+``)
- URI scheme ``ant://<channel>``
"""

from __future__ import annotations

from typing import ClassVar

from .._carriers.lazy import _LazyDatagramRPCProtocol


class _LAILA_IDENTIFIABLE_ANT_COMM_PROTOCOL(_LazyDatagramRPCProtocol):
    """ANT / ANT+ transport."""

    protocol_name: ClassVar[str] = "ant"
    _TOKEN_ALIASES: ClassVar[frozenset[str]] = frozenset({"ant", "ant+"})
    _DRIVER_MODULES: ClassVar[tuple] = ("openant",)
    _DRIVER_EXTRA: ClassVar[str] = "ant"

    @classmethod
    def matches_token(cls, token: str) -> bool:
        """Accept ``"ant"`` / ``"ant+"``."""
        return token.lower() in cls._TOKEN_ALIASES

    @classmethod
    def can_handle_uri(cls, uri: str) -> bool:
        """Claim ``ant://`` URIs."""
        return uri.startswith("ant://")
