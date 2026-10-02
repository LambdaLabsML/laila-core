"""Peer registry and channel-aware peer proxy.

:class:`PeerRegistry` is the ``dict`` subtype stored on
``communication.peers``. It keeps exact dict semantics (``len``,
iteration, ``in``, ``.get`` / ``.pop`` / ``.clear``) for every existing
caller and adds exactly one resolver, :meth:`PeerRegistry.channel`,
which turns ``(peer, name, transport-selector)`` into a
:class:`~.channel.Channel` by asking the carrier that holds the peer.
It holds no live stream state and no back-reference to the
communication object.

:class:`PeerProxy` is the value type stored in the registry. It is a
:class:`~.proxy.RemotePolicyProxy` that additionally supports item
access::

    laila.peers[gid]["default"]      # the ordinary RPC proxy itself
    laila.peers[gid]["video"]        # a Channel (opened lazily, cached)
    laila.peers[gid].via("uart")["video"]

Because dunder lookup happens on the type, ``__getitem__`` coexists with
the proxy's catch-all ``__getattr__``; ``proxy.central.memory.remember()``
is still a single RPC.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from .proxy import RemotePolicyProxy

if TYPE_CHECKING:
    from .channel import Channel
    from .schema.base import _LAILA_IDENTIFIABLE_COMMUNICATION

#: Reserved channel name that resolves to the RPC proxy itself.
DEFAULT_CHANNEL = "default"


class PeerRegistry(dict):
    """``dict[str, PeerProxy]`` with a single channel resolver.

    Notes
    -----
    Channel names never select a transport; that stays on
    :meth:`PeerProxy.via` / ``laila.request(gid, comm_protocol=...)``.
    """

    __slots__ = ()

    def channel(
        self,
        comm: _LAILA_IDENTIFIABLE_COMMUNICATION,
        peer_id: str,
        name: str,
        selector: Any = None,
    ) -> Channel:
        """Resolve ``(peer_id, name)`` to a :class:`Channel` on the right carrier.

        1. ``proto = comm._select_protocol_for_peer(peer_id, selector)``.
        2. Require ``type(proto).supports_channels``.
        3. Return ``proto.open_channel(peer_id, name)`` (cached inside the
           carrier; the first call may perform one blocking RPC).

        Raises
        ------
        ConnectionError
            If no (matching) transport holds the peer, the transport has
            no stream lanes, or the peer cannot open the lane.
        ValueError
            If *name* is the reserved ``"default"``.
        """
        if name == DEFAULT_CHANNEL:
            raise ValueError(f"{DEFAULT_CHANNEL!r} is the RPC proxy, not a stream channel.")
        proto = comm._select_protocol_for_peer(peer_id, selector)
        if not getattr(type(proto), "supports_channels", False):
            raise ConnectionError(
                f"{type(proto).protocol_name!r} has no stream lanes; use a stream-capable "
                "transport (e.g. tcp://, serial, loopback) for laila.peers[gid][name]."
            )
        return proto.open_channel(peer_id, name)


class PeerProxy(RemotePolicyProxy):
    """A :class:`RemotePolicyProxy` with ``[name]`` channel access.

    Adds only methods -- no new instance attributes -- so identity checks
    (``isinstance(x, RemotePolicyProxy)``), :func:`laila.activate_policy`
    (morph mode) and :func:`laila.request` keep working unchanged.

    Notes
    -----
    ``proxy[name]`` is *not* a cheap dict lookup: the first access to a
    negotiated lane performs one blocking RPC (bounded by the carrier's
    ``rpc_timeout``) and may raise ``ConnectionError``. Later accesses hit
    the carrier's cache.
    """

    __slots__ = ()

    # Defining __getitem__ would otherwise make the legacy sequence
    # protocol kick in for iter()/`in`; proxies are not iterable.
    __iter__ = None  # type: ignore[assignment]

    def __getitem__(self, name: str) -> Any:
        """``"default"`` -> this proxy; any other name -> a :class:`Channel`."""
        if not isinstance(name, str):
            raise TypeError(
                f"Channel names are strings, got {type(name).__name__}; "
                "use 'default' for the RPC proxy."
            )
        if name == DEFAULT_CHANNEL:
            return self
        return self._comm.peers.channel(self._comm, self._peer_id, name, self._comm_selector)

    def channels(self) -> list[str]:
        """Names of channels currently open to this peer (union over carriers).

        Never opens anything. With a ``via()`` selector only that
        transport is consulted.
        """
        comm = self._comm
        names: list[str] = []
        if self._comm_selector is not None:
            protos = [comm._select_protocol_for_peer(self._peer_id, self._comm_selector)]
        else:
            protos = [p for p in comm.connections.values() if p.has_peer(self._peer_id)]
        for proto in protos:
            fn = getattr(proto, "channel_names", None)
            if fn is None:
                continue
            for n in fn(self._peer_id):
                if n not in names:
                    names.append(n)
        return names

    def via(self, comm: Any) -> PeerProxy:
        """Return a :class:`PeerProxy` bound to transport *comm* (see base class)."""
        return PeerProxy(self._peer_id, self._comm, comm_selector=comm)

    def __repr__(self) -> str:
        if self._comm_selector is not None:
            return f"PeerProxy({self._peer_id!r}, via={self._comm_selector!r})"
        return f"PeerProxy({self._peer_id!r})"
