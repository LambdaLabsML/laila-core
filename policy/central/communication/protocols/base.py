"""Abstract base class for all communication protocol implementations."""

from __future__ import annotations

from typing import Any

from pydantic import PrivateAttr, ConfigDict

from .....basics.definitions.cli_capable import _LAILA_CLI_CAPABLE_CLASS
from .....atomic.definitions.locally_atomic_identifiable_object import (
    _LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT,
)
from .....macros.strings import _COMM_PROTOCOL_SCOPE


class _LAILA_IDENTIFIABLE_COMM_PROTOCOL(
    _LAILA_CLI_CAPABLE_CLASS,
    _LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT,
):
    """Base class for transport-layer protocol implementations.

    Subclasses (TCP/IP, shared memory, InfiniBand, etc.) implement the
    abstract interface below.  Each protocol instance is registered on
    a ``_LAILA_IDENTIFIABLE_COMMUNICATION`` via ``add_connection()``
    which sets the ``_communication`` back-reference.
    """

    _scopes: list[str] = PrivateAttr(
        default_factory=lambda: [_COMM_PROTOCOL_SCOPE],
    )

    model_config = ConfigDict(arbitrary_types_allowed=True)

    _communication: Any = PrivateAttr(default=None)

    # ------------------------------------------------------------------
    # Abstract interface
    # ------------------------------------------------------------------

    @classmethod
    def can_handle_uri(cls, uri: str) -> bool:
        """Return ``True`` if this protocol can handle the given URI scheme."""
        return False

    def start(self) -> None:
        """Start listening for inbound connections."""
        raise NotImplementedError

    def stop(self) -> None:
        """Shut down all connections and release resources."""
        raise NotImplementedError

    def add_peer(self, uri: str, secret: str) -> str:
        """Connect to a remote policy and return its ``global_id``."""
        raise NotImplementedError

    def send_rpc(
        self, peer_id: str, path: list[str], args: tuple, kwargs: dict
    ) -> Any:
        """Send an RPC call to *peer_id* and block for the result."""
        raise NotImplementedError

    def has_peer(self, peer_id: str) -> bool:
        """Return ``True`` if this protocol holds a live connection to *peer_id*."""
        return False
