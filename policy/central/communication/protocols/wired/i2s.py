"""I2S (audio) communication transport (datagram, experimental).

I2S is a synchronous audio bus, not a packet link; carrying RPC over it
requires modulating data into an audio stream, which needs dedicated
DSP. This lazy datagram transport raises a clear requirement error on
start.

- ``protocol_name`` ``"i2s"``
- URI scheme ``i2s://<card>``
"""

from __future__ import annotations

from typing import ClassVar

from .._carriers.lazy import _LazyDatagramRPCProtocol


class _LAILA_IDENTIFIABLE_I2S_COMM_PROTOCOL(_LazyDatagramRPCProtocol):
    """I2S audio-bus transport (experimental)."""

    protocol_name: ClassVar[str] = "i2s"
    _TOKEN_ALIASES: ClassVar[frozenset[str]] = frozenset({"i2s"})

    @classmethod
    def can_handle_uri(cls, uri: str) -> bool:
        """Claim ``i2s://`` URIs."""
        return uri.startswith("i2s://")
