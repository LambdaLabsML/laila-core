"""Abstract base class for communication-protocol implementations.

A *protocol* is the transport layer that sits between
:class:`_LAILA_IDENTIFIABLE_COMMUNICATION` and the wire. Concrete
subclasses know how to:

- Open a listener that accepts inbound peering handshakes.
- Initiate outbound peering handshakes against a URI + secret.
- Encode RPC frames (``path`` + ``args`` + ``kwargs``), send them to
  a known peer, and decode the response.
- Tear themselves down cleanly on :meth:`stop`.

The base class only fixes the *shape* of that contract -- the
specifics (TCP/IP via WebSockets, shared-memory queues, InfiniBand,
gRPC, ...) are entirely up to the subclass.

Each protocol instance carries a ``_communication`` back-reference set
by :meth:`_LAILA_IDENTIFIABLE_COMMUNICATION.add_connection` so it can
forward inbound RPCs back to the policy and notify the communication
layer when peers connect or disconnect.
"""

from __future__ import annotations

from typing import Any, ClassVar

from pydantic import ConfigDict, PrivateAttr

from .....atomic.definitions.locally_atomic_identifiable_object import (
    _LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT,
)
from .....basics.definitions.cli_capable import _LAILA_CLI_CAPABLE_CLASS
from .....macros.strings import _COMM_PROTOCOL_SCOPE


class _LAILA_IDENTIFIABLE_COMM_PROTOCOL(
    _LAILA_CLI_CAPABLE_CLASS,
    _LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT,
):
    """Base class for transport-layer protocol implementations.

    Subclasses (TCP/IP, shared memory, InfiniBand, etc.) implement the
    abstract interface below. Each protocol instance is registered on
    a :class:`_LAILA_IDENTIFIABLE_COMMUNICATION` via
    :meth:`add_connection`, which sets the ``_communication``
    back-reference and calls :meth:`start`.

    Notes
    -----
    All public lifecycle methods are required to be *idempotent*:

    - Calling :meth:`start` on a running protocol should be a no-op.
    - Calling :meth:`stop` on a stopped protocol should be a no-op.

    This keeps higher-level code in :class:`_LAILA_IDENTIFIABLE_COMMUNICATION`
    free of "is this protocol already up?" book-keeping.
    """

    _scopes: list[str] = PrivateAttr(
        default_factory=lambda: [_COMM_PROTOCOL_SCOPE],
    )

    model_config = ConfigDict(arbitrary_types_allowed=True)

    _communication: Any = PrivateAttr(default=None)

    #: Short, stable token identifying this transport family (e.g.
    #: ``"tcpip"``, ``"lora"``, ``"bluetooth"``). It is the value users
    #: pass as ``comm_protocol`` to :func:`laila.request` and the key
    #: :meth:`_LAILA_IDENTIFIABLE_COMMUNICATION._resolve_protocol_for_token`
    #: matches against. Subclasses MUST override it.
    protocol_name: ClassVar[str] = "base"

    # ------------------------------------------------------------------
    # Abstract interface
    # ------------------------------------------------------------------

    @classmethod
    def matches_token(cls, token: str) -> bool:
        """Return ``True`` if this protocol answers to the transport *token*.

        The default compares *token* case-insensitively against
        :attr:`protocol_name`. Subclasses may override to accept
        aliases (e.g. ``"tcp"`` / ``"tcp-ip"`` for the TCP/IP protocol).

        Used by
        :meth:`_LAILA_IDENTIFIABLE_COMMUNICATION._resolve_protocol_for_token`
        to dispatch :func:`laila.request` over the requested transport.
        """
        return token.lower() == cls.protocol_name.lower()

    @classmethod
    def can_handle_uri(cls, uri: str) -> bool:
        """Return ``True`` if this protocol claims responsibility for *uri*.

        Used by :meth:`_LAILA_IDENTIFIABLE_COMMUNICATION._resolve_protocol_for_uri`
        to dispatch outbound peering. Subclasses typically inspect the
        URI scheme, e.g. ``uri.startswith("ws://")``.

        The default implementation returns ``False`` -- subclasses must
        opt in.
        """
        return False

    def start(self) -> None:
        """Start the protocol's listener loop and any background workers.

        Subclasses must implement; the base raises
        :class:`NotImplementedError`.
        """
        raise NotImplementedError

    def stop(self) -> None:
        """Tear down all connections and release transport resources.

        Subclasses must implement; the base raises
        :class:`NotImplementedError`. Implementations should close
        open connections, terminate worker threads, and call
        :meth:`_LAILA_IDENTIFIABLE_COMMUNICATION._unregister_peer`
        for each peer that goes away.
        """
        raise NotImplementedError

    def add_peer(self, uri: str, secret: str) -> str:
        """Initiate an outbound peering handshake to *uri* using *secret*.

        On success, the protocol must call
        :meth:`_LAILA_IDENTIFIABLE_COMMUNICATION._register_peer` so a
        :class:`RemotePolicyProxy` is created in the local registry.

        Returns
        -------
        str
            The remote policy's ``global_id``.
        """
        raise NotImplementedError

    def send_rpc(self, peer_id: str, path: list[str], args: tuple, kwargs: dict) -> Any:
        """Send an RPC frame to *peer_id* and block for the deserialized response.

        The frame layout is the protocol's choice; the only contract
        is that the remote :meth:`_LAILA_IDENTIFIABLE_COMMUNICATION._execute_rpc`
        is invoked with ``path``, ``args``, ``kwargs`` and that the
        result (or a future-shaped envelope) flows back here.
        """
        raise NotImplementedError

    def has_peer(self, peer_id: str) -> bool:
        """Return ``True`` if this protocol currently holds a live connection to *peer_id*.

        Used by :meth:`_LAILA_IDENTIFIABLE_COMMUNICATION._send_rpc` to
        pick the right protocol when more than one is registered.
        """
        return False
