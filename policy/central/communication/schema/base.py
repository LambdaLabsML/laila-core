"""Core communication sub-system for inter-policy peer-to-peer RPC.

Manages a registry of transport protocols (TCP/IP, shared memory, etc.)
and a protocol-agnostic peer registry.  Delegates all transport-level
work to the protocol instances stored in ``connections``.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from pydantic import PrivateAttr, ConfigDict
from .....basics.definitions.cli_capable import CLIExempt, _LAILA_CLI_CAPABLE_CLASS

from .....basics.definitions.identifiable_object import _LAILA_IDENTIFIABLE_OBJECT
from .....macros.strings import _CENTRAL_COMMUNICATION_SCOPE
from ..proxy import RemotePolicyProxy
from ..protocols.base import _LAILA_IDENTIFIABLE_COMM_PROTOCOL

log = logging.getLogger(__name__)


class _LAILA_IDENTIFIABLE_COMMUNICATION(_LAILA_CLI_CAPABLE_CLASS, _LAILA_IDENTIFIABLE_OBJECT):
    """Central communication that owns transport protocols and a peer registry.

    Parameters
    ----------
    policy_id : str, optional
        ``global_id`` of the owning policy (set automatically during wiring).
    """

    _scopes: list[str] = PrivateAttr(
        default_factory=lambda: [_CENTRAL_COMMUNICATION_SCOPE],
    )

    model_config = ConfigDict(arbitrary_types_allowed=True)

    policy_id: Optional[str] = CLIExempt(default=None)
    peers: Dict[str, RemotePolicyProxy] = CLIExempt(default_factory=dict)
    connections: Dict[str, _LAILA_IDENTIFIABLE_COMM_PROTOCOL] = CLIExempt(
        default_factory=dict,
    )

    _local_policy: Any = PrivateAttr(default=None)

    # ------------------------------------------------------------------
    # Protocol management
    # ------------------------------------------------------------------

    def add_connection(
        self, protocol: _LAILA_IDENTIFIABLE_COMM_PROTOCOL
    ) -> None:
        """Register a transport protocol with this communication instance.

        Sets the protocol's back-reference so it can call back into
        Communication for peer registration and RPC dispatch.

        Parameters
        ----------
        protocol : _LAILA_IDENTIFIABLE_COMM_PROTOCOL
            The protocol instance to register.
        """
        protocol._communication = self
        self.connections[protocol.global_id] = protocol

    def _resolve_protocol_for_uri(self, uri: str) -> _LAILA_IDENTIFIABLE_COMM_PROTOCOL:
        """Find a protocol that can handle *uri*, or fall back to the first one."""
        if not self.connections:
            raise ConnectionError(
                "No communication protocols configured. "
                "Call add_connection() first."
            )
        for proto in self.connections.values():
            if type(proto).can_handle_uri(uri):
                return proto
        return next(iter(self.connections.values()))

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Start all registered protocols."""
        for proto in self.connections.values():
            proto.start()
        log.info("Communication started for policy %s", self.policy_id)

    def stop(self) -> None:
        """Stop all registered protocols and clear peers."""
        for proto in self.connections.values():
            proto.stop()
        self.peers.clear()
        log.info("Communication stopped for policy %s", self.policy_id)

    # ------------------------------------------------------------------
    # Peer management
    # ------------------------------------------------------------------

    def add_peer(self, uri: str, secret: str) -> str:
        """Initiate a peering connection to a remote policy.

        Resolves the appropriate protocol for *uri* and delegates the
        transport-level handshake.

        Parameters
        ----------
        uri : str
            URI of the remote policy (e.g. ``"ws://host:port"``).
        secret : str
            The remote policy's ``peer_secret_key``.

        Returns
        -------
        str
            The ``global_id`` of the newly peered remote policy.
        """
        proto = self._resolve_protocol_for_uri(uri)
        return proto.add_peer(uri, secret)

    def _register_peer(self, peer_id: str) -> None:
        """Create a proxy for a newly connected peer.

        Called by protocol instances after a successful handshake.

        Parameters
        ----------
        peer_id : str
            Remote policy ``global_id``.
        """
        if peer_id not in self.peers:
            self.peers[peer_id] = RemotePolicyProxy(peer_id, self)

    def _unregister_peer(self, peer_id: str) -> None:
        """Remove a peer proxy after the transport connection closes.

        Parameters
        ----------
        peer_id : str
            Remote policy ``global_id``.
        """
        self.peers.pop(peer_id, None)

    # ------------------------------------------------------------------
    # RPC dispatch (inbound)
    # ------------------------------------------------------------------

    def _execute_rpc(self, path: list[str], args: list, kwargs: dict) -> Any:
        """Execute a dotted-path method call on the local policy.

        Parameters
        ----------
        path : list[str]
            Attribute chain relative to the local policy object,
            e.g. ``["central", "memory", "memorize"]``.
        args : list
            Positional arguments.
        kwargs : dict
            Keyword arguments.

        Returns
        -------
        Any
            Return value of the invoked method.

        Raises
        ------
        AttributeError
            If any segment of *path* does not exist on the target.
        """
        obj = self._local_policy
        if obj is None:
            raise RuntimeError("Communication has no reference to the local policy.")
        for segment in path:
            obj = getattr(obj, segment)
        return obj(*args, **kwargs)

    # ------------------------------------------------------------------
    # RPC dispatch (outbound)
    # ------------------------------------------------------------------

    def _send_rpc(self, peer_id: str, path: list[str], args: tuple, kwargs: dict) -> Any:
        """Send an RPC call to a peer via the protocol that holds the connection.

        Parameters
        ----------
        peer_id : str
            Target peer ``global_id``.
        path : list[str]
            Dotted attribute chain on the remote policy.
        args : tuple
            Positional arguments.
        kwargs : dict
            Keyword arguments.

        Returns
        -------
        Any
            The deserialized return value from the remote call.

        Raises
        ------
        ConnectionError
            If no protocol holds a connection to *peer_id*.
        RuntimeError
            If the remote side returned an error.
        """
        for proto in self.connections.values():
            if proto.has_peer(peer_id):
                return proto.send_rpc(peer_id, path, args, kwargs)
        raise ConnectionError(f"No connection to peer {peer_id}")
