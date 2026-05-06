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
        """Register and start a transport protocol.

        Sets the protocol's back-reference, adds it to the registry,
        and calls ``protocol.start()`` so the connection is live when
        this method returns.  Symmetric with :meth:`remove_connection`
        which calls ``protocol.stop()``.

        Parameters
        ----------
        protocol : _LAILA_IDENTIFIABLE_COMM_PROTOCOL
            The protocol instance to register and start.
        """
        protocol._communication = self
        self.connections[protocol.global_id] = protocol
        protocol.start()

    def remove_connection(
        self, protocol: _LAILA_IDENTIFIABLE_COMM_PROTOCOL
    ) -> None:
        """Stop a transport protocol and remove it from this communication instance.

        The protocol is responsible for all its own cleanup — closing
        sockets, unregistering peers, etc.  Communication only calls
        ``stop()`` and removes the protocol from its registry.

        Parameters
        ----------
        protocol : _LAILA_IDENTIFIABLE_COMM_PROTOCOL
            The protocol instance to remove.
        """
        proto_id = protocol.global_id
        if proto_id not in self.connections:
            return
        protocol.stop()
        self.connections.pop(proto_id, None)
        protocol._communication = None

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

    def add_tcpip_peer(self, host: str, port: int, secret: str) -> str:
        """Peer with a remote policy over TCP/IP (WebSocket).

        Convenience wrapper that builds the ``ws://`` URI internally.

        Parameters
        ----------
        host : str
            Hostname or IP address of the remote node.
        port : int
            WebSocket port on the remote node.
        secret : str
            The remote node's ``peer_secret_key``.

        Returns
        -------
        str
            The ``global_id`` of the newly peered remote policy.
        """
        return self.add_peer(f"ws://{host}:{port}", secret)

    def _register_peer(self, peer_id: str) -> None:
        """Create a proxy for a newly connected peer.

        Called by protocol instances after a successful handshake.
        Also registers the proxy in ``laila.remote_policies``.

        Parameters
        ----------
        peer_id : str
            Remote policy ``global_id``.
        """
        if peer_id not in self.peers:
            proxy = RemotePolicyProxy(peer_id, self)
            self.peers[peer_id] = proxy
            from ..... import _remote_policies
            _remote_policies[peer_id] = proxy

    def _unregister_peer(self, peer_id: str) -> None:
        """Remove a peer proxy after the transport connection closes.

        Also removes from ``laila.remote_policies``.

        Parameters
        ----------
        peer_id : str
            Remote policy ``global_id``.
        """
        self.peers.pop(peer_id, None)
        from ..... import _remote_policies
        _remote_policies.pop(peer_id, None)

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

        If the deserialized response contains a ``__laila_future__`` marker
        it is automatically wrapped in a :class:`RemoteFuture` and
        registered in the local policy's ``future_bank``.

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
            The deserialized return value from the remote call, or a
            ``RemoteFuture`` when the remote returned a future.

        Raises
        ------
        ConnectionError
            If no protocol holds a connection to *peer_id*.
        RuntimeError
            If the remote side returned an error.
        """
        for proto in self.connections.values():
            if proto.has_peer(peer_id):
                result = proto.send_rpc(peer_id, path, args, kwargs)
                return self._maybe_wrap_remote_future(result, peer_id)
        raise ConnectionError(f"No connection to peer {peer_id}")

    def _maybe_wrap_remote_future(self, result: Any, peer_id: str) -> Any:
        """Wrap a future-shaped dict in a ``RemoteFuture`` pydantic handle.

        ``RemoteFuture.model_post_init`` self-registers into the active
        policy's ``future_bank`` and guarantee stack, so this helper only
        builds the instance and binds the communication channel.
        """
        if not isinstance(result, dict) or not result.get("__laila_future__"):
            return result

        from ...command.schema.future.future.remote_future import RemoteFuture
        from .....basics.definitions.identifiable_object import GLOBAL_ID_REGEX_PATTERN
        from .....macros.strings import _FUTURE_SCOPE, _GROUP_FUTURE_SCOPE

        remote_gid = result["global_id"]
        match = GLOBAL_ID_REGEX_PATTERN.match(remote_gid)
        if match is None:
            raise ValueError(f"Invalid remote future gid: {remote_gid!r}")
        remote_uuid = match.group("uuid")
        evolution_raw = match.group("evolution")
        evolution = int(evolution_raw) if evolution_raw is not None else None
        is_group = bool(result.get("__is_group__", False))

        rf = RemoteFuture(
            taskforce_id=result.get("taskforce_id", peer_id),
            policy_id=result.get("policy_id", peer_id),
            uuid=remote_uuid,
            scopes=[_GROUP_FUTURE_SCOPE if is_group else _FUTURE_SCOPE],
            evolution=evolution,
        )
        rf.bind(self, is_group=is_group)
        return rf
