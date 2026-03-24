"""Transparent remote-policy proxy for inter-policy RPC.

``RemotePolicyProxy`` duck-types enough of ``_LAILA_IDENTIFIABLE_POLICY``
that it can be passed to ``laila.activate_policy``.  Attribute access
chains are lazily accumulated by ``_RemoteAttrChain`` and only trigger a
network round-trip when the terminal ``__call__`` is invoked.
"""

from __future__ import annotations

from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from .schema.base import _LAILA_IDENTIFIABLE_COMMUNICATION


class RemotePolicyProxy:
    """Client-side proxy representing a peered remote policy.

    Exposes ``global_id`` so that ``activate_policy`` and other identity
    checks work transparently.  All other attribute access is forwarded
    over the wire.

    Parameters
    ----------
    peer_id : str
        The remote policy's ``global_id``.
    communication : _LAILA_IDENTIFIABLE_COMMUNICATION
        The local communication instance that owns the WebSocket to
        *peer_id*.
    """

    __slots__ = ("_peer_id", "_comm")

    def __init__(self, peer_id: str, communication: _LAILA_IDENTIFIABLE_COMMUNICATION) -> None:
        object.__setattr__(self, "_peer_id", peer_id)
        object.__setattr__(self, "_comm", communication)

    @property
    def global_id(self) -> str:
        """Return the remote policy's ``global_id``."""
        return self._peer_id

    def __getattr__(self, name: str) -> _RemoteAttrChain:
        """Begin a remote attribute chain."""
        return _RemoteAttrChain(self._comm, self._peer_id, [name])

    def __repr__(self) -> str:
        return f"RemotePolicyProxy({self._peer_id!r})"


class _RemoteAttrChain:
    """Accumulates dotted attribute path and sends an RPC on ``__call__``.

    Parameters
    ----------
    communication : _LAILA_IDENTIFIABLE_COMMUNICATION
        Local communication instance.
    peer_id : str
        Target peer ``global_id``.
    path : list[str]
        Attribute segments accumulated so far.
    """

    __slots__ = ("_comm", "_peer_id", "_path")

    def __init__(self, communication: _LAILA_IDENTIFIABLE_COMMUNICATION, peer_id: str, path: list[str]) -> None:
        object.__setattr__(self, "_comm", communication)
        object.__setattr__(self, "_peer_id", peer_id)
        object.__setattr__(self, "_path", path)

    def __getattr__(self, name: str) -> _RemoteAttrChain:
        """Extend the attribute chain by one segment."""
        return _RemoteAttrChain(self._comm, self._peer_id, self._path + [name])

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        """Send the accumulated path + arguments as an RPC call and block for the result."""
        return self._comm._send_rpc(self._peer_id, self._path, args, kwargs)

    def __repr__(self) -> str:
        dotted = ".".join(self._path)
        return f"_RemoteAttrChain({self._peer_id!r}, {dotted!r})"
