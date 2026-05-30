"""Transparent remote-policy proxy for inter-policy RPC.

A :class:`RemotePolicyProxy` is the local stand-in for a *remote*
policy. From the caller's point of view it looks (and quacks) like a
local :class:`_LAILA_IDENTIFIABLE_POLICY` -- you can pass it to
:func:`laila.activate_policy`, query its ``global_id``, and reach
its ``central.memory.memorize`` / ``central.command.submit`` methods
exactly as though it were local. The difference is that every
terminal call is sent over the wire instead of executed in-process.

The trick that keeps the call site clean is *attribute-chain
accumulation* via :class:`_RemoteAttrChain`. Each attribute access on
a proxy returns a chain object that records the dotted path so far
without doing any I/O. Only when the chain is finally invoked
(``__call__``) does a single RPC frame get serialised and dispatched
through the communication layer. That means

    proxy.central.memory.remember("x")

results in exactly one network round-trip, with the remote side
receiving ``["central", "memory", "remember"]`` plus
``args=("x",), kwargs={}``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .schema.base import _LAILA_IDENTIFIABLE_COMMUNICATION


class RemotePolicyProxy:
    """Client-side proxy representing a peered remote policy.

    Exposes ``global_id`` so that :func:`laila.activate_policy` and
    other identity checks work transparently. All *other* attribute
    access is captured as the start of a chain
    (:class:`_RemoteAttrChain`) and only triggers a network round-trip
    when the chain is finally invoked.

    Parameters
    ----------
    peer_id : str
        The remote policy's ``global_id``.
    communication : _LAILA_IDENTIFIABLE_COMMUNICATION
        The local central-communication instance that owns the
        underlying transport to *peer_id*.

    Notes
    -----
    Storage is kept in ``__slots__`` so the proxy is cheap to allocate
    and writes via ``object.__setattr__`` to bypass our own
    ``__getattr__`` (which would otherwise capture the assignment as
    the start of a remote attribute chain!).
    """

    __slots__ = ("_comm", "_peer_id")

    def __init__(self, peer_id: str, communication: _LAILA_IDENTIFIABLE_COMMUNICATION) -> None:
        object.__setattr__(self, "_peer_id", peer_id)
        object.__setattr__(self, "_comm", communication)

    @property
    def global_id(self) -> str:
        """The remote policy's ``global_id`` (resolved without I/O)."""
        return self._peer_id

    def __getattr__(self, name: str) -> _RemoteAttrChain:
        """Begin a remote attribute chain rooted at *name*.

        Called for any attribute that is not in ``__slots__`` and is
        not the explicit :attr:`global_id` property. Returns a
        :class:`_RemoteAttrChain`; further attribute accesses extend
        the path, ``__call__`` materialises the RPC.
        """
        return _RemoteAttrChain(self._comm, self._peer_id, [name])

    def __repr__(self) -> str:
        return f"RemotePolicyProxy({self._peer_id!r})"


class _RemoteAttrChain:
    """Accumulator for a dotted attribute path; flushes as an RPC on ``__call__``.

    Each attribute access appends to the path *immutably* (a new
    chain object is returned, the existing one is unchanged), so the
    chain is safe to share across threads. Calling the chain sends
    one RPC frame containing the full path plus the call's positional
    and keyword arguments.

    Parameters
    ----------
    communication : _LAILA_IDENTIFIABLE_COMMUNICATION
        Local communication instance used to dispatch the RPC.
    peer_id : str
        Target peer ``global_id``.
    path : list[str]
        Attribute segments accumulated so far. Should not be mutated
        in place by callers; a new list is created per ``__getattr__``.
    """

    __slots__ = ("_comm", "_path", "_peer_id")

    def __init__(
        self, communication: _LAILA_IDENTIFIABLE_COMMUNICATION, peer_id: str, path: list[str]
    ) -> None:
        object.__setattr__(self, "_comm", communication)
        object.__setattr__(self, "_peer_id", peer_id)
        object.__setattr__(self, "_path", path)

    def __getattr__(self, name: str) -> _RemoteAttrChain:
        """Return a *new* chain whose path is the current path plus *name*."""
        return _RemoteAttrChain(self._comm, self._peer_id, self._path + [name])

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        """Dispatch the accumulated path + arguments as a single RPC.

        Blocks the calling thread until the remote responds. If the
        remote returns a future-shaped envelope, the communication
        layer transparently wraps it in a :class:`RemoteFuture`
        before returning.
        """
        return self._comm._send_rpc(self._peer_id, self._path, args, kwargs)

    def __repr__(self) -> str:
        dotted = ".".join(self._path)
        return f"_RemoteAttrChain({self._peer_id!r}, {dotted!r})"
