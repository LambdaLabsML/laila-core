"""Thread communication transport (datagram radio).

Carries fragmented JSON-RPC over a Thread mesh via OpenThread's
``pyspinel`` and an NCP radio. Built on the lazy datagram carrier.

- ``protocol_name`` ``"thread"``
- URI scheme ``thread://<node>``
"""

from __future__ import annotations

from typing import ClassVar

from .._carriers.lazy import _LazyDatagramRPCProtocol


class _LAILA_IDENTIFIABLE_THREAD_COMM_PROTOCOL(_LazyDatagramRPCProtocol):
    """Thread mesh transport."""

    protocol_name: ClassVar[str] = "thread"
    _TOKEN_ALIASES: ClassVar[frozenset[str]] = frozenset({"thread"})
    _DRIVER_MODULES: ClassVar[tuple] = ("spinel",)
    _DRIVER_EXTRA: ClassVar[str] = "thread"

    @classmethod
    def can_handle_uri(cls, uri: str) -> bool:
        """Claim ``thread://`` URIs."""
        return uri.startswith("thread://")
