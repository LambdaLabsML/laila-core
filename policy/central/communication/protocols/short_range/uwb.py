"""UWB (ultra-wideband ranging) communication transport (datagram radio).

UWB modules are vendor-specific with no common host-side Python driver,
so this lazy datagram transport raises a clear hardware-requirement
error on start.

- ``protocol_name`` ``"uwb"``
- URI scheme ``uwb://<anchor>``
"""

from __future__ import annotations

from typing import ClassVar

from .._carriers.lazy import _LazyDatagramRPCProtocol


class _LAILA_IDENTIFIABLE_UWB_COMM_PROTOCOL(_LazyDatagramRPCProtocol):
    """UWB transport (vendor hardware)."""

    protocol_name: ClassVar[str] = "uwb"
    _TOKEN_ALIASES: ClassVar[frozenset[str]] = frozenset({"uwb"})

    @classmethod
    def can_handle_uri(cls, uri: str) -> bool:
        """Claim ``uwb://`` URIs."""
        return uri.startswith("uwb://")
