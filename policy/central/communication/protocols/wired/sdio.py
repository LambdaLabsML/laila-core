"""SDIO communication transport (datagram).

SDIO is a block/register card interface, not a symmetric peer link, and
has no userspace peer-RPC driver, so this lazy datagram transport raises
a clear hardware-requirement error on start.

- ``protocol_name`` ``"sdio"``
- URI scheme ``sdio://<func>``
"""

from __future__ import annotations

from typing import ClassVar

from .._carriers.lazy import _LazyDatagramRPCProtocol


class _LAILA_IDENTIFIABLE_SDIO_COMM_PROTOCOL(_LazyDatagramRPCProtocol):
    """SDIO transport."""

    protocol_name: ClassVar[str] = "sdio"
    _TOKEN_ALIASES: ClassVar[frozenset[str]] = frozenset({"sdio"})

    @classmethod
    def can_handle_uri(cls, uri: str) -> bool:
        """Claim ``sdio://`` URIs."""
        return uri.startswith("sdio://")
