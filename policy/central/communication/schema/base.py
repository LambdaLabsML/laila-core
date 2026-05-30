"""Core communication sub-system for inter-policy peer-to-peer RPC.

The :class:`_LAILA_IDENTIFIABLE_COMMUNICATION` class is the *central
communication* subsystem of every laila :class:`Policy`. Its job is to
mediate calls between local and remote policies without any of the
upstream code having to know which transport is in play. Concretely it
owns three things:

- A *protocol registry* (``connections``): one or more transport-level
  drivers (TCP/IP today, shared memory or queues in the future). Each
  protocol implements its own listener loop, peer-handshake, and wire
  encoding. The communication object only ever asks "do you handle
  this URI?" or "do you have this peer connected?" -- everything else
  is delegated.
- A *peer registry* (``peers``): a map from remote-policy ``global_id``
  to a :class:`RemotePolicyProxy`. Proxies look like local policies
  to the rest of the codebase but route every method call through
  :meth:`_send_rpc`.
- An *inbound dispatcher* (:meth:`_execute_rpc`): when a protocol
  finishes deserializing an incoming RPC frame it hands the dotted
  attribute path + args/kwargs back to the communication object,
  which walks the path on the local policy and invokes the resolved
  method.

The class is also responsible for *future virtualisation*: when a
remote call returns a future-shaped envelope (as marked by the
``__laila_future__`` key) it transparently wraps the envelope in a
:class:`RemoteFuture` that proxies status / wait / result calls back
to the originating peer (see :meth:`_maybe_wrap_remote_future`).
"""

from __future__ import annotations

import logging
from typing import Any

from pydantic import ConfigDict, PrivateAttr

from .....basics.definitions.cli_capable import _LAILA_CLI_CAPABLE_CLASS, CLIExempt
from .....basics.definitions.identifiable_object import _LAILA_IDENTIFIABLE_OBJECT
from .....macros.strings import _CENTRAL_COMMUNICATION_SCOPE
from ..protocols.base import _LAILA_IDENTIFIABLE_COMM_PROTOCOL
from ..proxy import RemotePolicyProxy

log = logging.getLogger(__name__)


class _LAILA_IDENTIFIABLE_COMMUNICATION(_LAILA_CLI_CAPABLE_CLASS, _LAILA_IDENTIFIABLE_OBJECT):
    """Central-communication hub for a policy.

    Owns transport protocols, the peer registry, and an inbound RPC
    dispatcher. The full interaction loop is::

        local user code
            -> RemotePolicyProxy.foo()
            -> Communication._send_rpc(...)
            -> Protocol.send_rpc(...)
            ~~ wire ~~
            -> remote Protocol receives, decodes
            -> remote Communication._execute_rpc(["foo"], args, kwargs)
            -> result returned along the same path in reverse

    Parameters
    ----------
    policy_id : str, optional
        ``global_id`` of the owning policy. Wired automatically by the
        policy during construction; only set manually in tests or
        when a communication object lives outside a policy.

    Attributes
    ----------
    peers : dict[str, RemotePolicyProxy]
        Remote-policy ``global_id`` -> proxy. Populated by
        :meth:`_register_peer` after a successful handshake.
    connections : dict[str, _LAILA_IDENTIFIABLE_COMM_PROTOCOL]
        Protocol ``global_id`` -> protocol instance. Each protocol
        manages its own connections, listeners and peer set; this
        map is just a registry that lets the communication layer
        find the right transport for a URI or peer.
    """

    _scopes: list[str] = PrivateAttr(
        default_factory=lambda: [_CENTRAL_COMMUNICATION_SCOPE],
    )

    model_config = ConfigDict(arbitrary_types_allowed=True)

    policy_id: str | None = CLIExempt(default=None)
    peers: dict[str, RemotePolicyProxy] = CLIExempt(default_factory=dict)
    connections: dict[str, _LAILA_IDENTIFIABLE_COMM_PROTOCOL] = CLIExempt(
        default_factory=dict,
    )

    _local_policy: Any = PrivateAttr(default=None)

    # ------------------------------------------------------------------
    # Protocol management
    # ------------------------------------------------------------------

    def add_connection(self, protocol: _LAILA_IDENTIFIABLE_COMM_PROTOCOL) -> None:
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

    def remove_connection(self, protocol: _LAILA_IDENTIFIABLE_COMM_PROTOCOL) -> None:
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
        """Find a registered protocol that can handle *uri*.

        Each protocol class implements a ``can_handle_uri`` classmethod
        (e.g. TCP/IP returns ``True`` for ``ws://`` and ``tcp://`` URIs).
        The first registered protocol that claims the URI is returned;
        if none claims it, the first registered protocol is returned
        as a best-effort fallback (the protocol may still error out).

        Raises
        ------
        ConnectionError
            If no protocols are registered.
        """
        if not self.connections:
            raise ConnectionError(
                "No communication protocols configured. Call add_connection() first."
            )
        for proto in self.connections.values():
            if type(proto).can_handle_uri(uri):
                return proto
        return next(iter(self.connections.values()))

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Start every registered protocol's listener loop.

        Idempotent at the protocol level -- calling :meth:`start` on an
        already-started protocol is a no-op. Logged at INFO so the
        policy lifecycle is auditable.
        """
        for proto in self.connections.values():
            proto.start()
        log.info("Communication started for policy %s", self.policy_id)

    def stop(self) -> None:
        """Stop every registered protocol and drop all peer proxies.

        Note that the protocols are responsible for breaking their
        own connections; this method just clears the in-memory peer
        registry afterwards so subsequent code does not try to talk
        to detached proxies.
        """
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
        """Promote a future-shaped envelope into a real :class:`RemoteFuture`.

        Detected by the ``__laila_future__`` flag on the deserialized
        result. The envelope carries enough identity information
        (uuid, evolution, scopes, taskforce/policy ids) to reconstruct
        a stable identity-only :class:`RemoteFuture` on the local side
        without round-tripping the underlying value. The new future
        is then bound to *self* so subsequent ``status``/``wait``/
        ``result`` calls route back to *peer_id*.

        :class:`RemoteFuture.model_post_init` self-registers into the
        active local policy's ``future_bank`` and guarantee stack, so
        this helper only has to build the instance and call
        :meth:`RemoteFuture.bind` to attach the communication channel.

        Group futures (``__is_group__``) are flagged so the bound
        proxy uses the GroupFuture-shaped status payload.
        """
        if not isinstance(result, dict) or not result.get("__laila_future__"):
            return result

        from .....basics.definitions.identifiable_object import GLOBAL_ID_REGEX_PATTERN
        from .....macros.strings import _FUTURE_SCOPE, _GROUP_FUTURE_SCOPE
        from ...command.schema.future.future.remote_future import RemoteFuture

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
