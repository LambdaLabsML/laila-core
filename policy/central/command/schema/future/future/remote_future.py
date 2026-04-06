"""Client-side proxy for a future that lives on a remote policy.

A ``RemoteFuture`` is created on the local side whenever an RPC call
returns a serialized future from a remote peer.  It delegates
``status``, ``result``, ``exception``, and ``wait`` queries back to the
remote policy over the same communication channel.
"""

from __future__ import annotations

from typing import Any, Optional, TYPE_CHECKING

from .future_status import FutureStatus

if TYPE_CHECKING:
    from ....communication.schema.base import _LAILA_IDENTIFIABLE_COMMUNICATION


class RemoteFuture:
    """Proxy for a future that lives on a remote policy's future bank.

    Parameters
    ----------
    remote_future_id : str
        The ``global_id`` of the future on the remote side.
    remote_policy_id : str
        The ``global_id`` of the remote policy that owns the future.
    communication : _LAILA_IDENTIFIABLE_COMMUNICATION
        The local communication instance that holds the connection to
        the remote peer.
    is_group : bool
        ``True`` when the remote future is a ``GroupFuture``.
    """

    __slots__ = (
        "_remote_future_id",
        "_remote_policy_id",
        "_comm",
        "_is_group",
    )

    def __init__(
        self,
        remote_future_id: str,
        remote_policy_id: str,
        communication: _LAILA_IDENTIFIABLE_COMMUNICATION,
        *,
        is_group: bool = False,
    ) -> None:
        object.__setattr__(self, "_remote_future_id", remote_future_id)
        object.__setattr__(self, "_remote_policy_id", remote_policy_id)
        object.__setattr__(self, "_comm", communication)
        object.__setattr__(self, "_is_group", is_group)

    @property
    def global_id(self) -> str:
        return self._remote_future_id

    @property
    def policy_id(self) -> str:
        return self._remote_policy_id

    @property
    def is_group(self) -> bool:
        return self._is_group

    @property
    def status(self) -> Any:
        """Query the remote policy for this future's status."""
        return self._comm._send_rpc(
            self._remote_policy_id,
            ["_get_future_status"],
            (self._remote_future_id,),
            {},
        )

    @property
    def result(self) -> Any:
        """Return the ``global_id`` of the result entry on the remote side.

        The caller should use ``laila.remember(result_gid, ...)`` to
        actually fetch the data through a shared pool.
        """
        return self._comm._send_rpc(
            self._remote_policy_id,
            ["_get_future_result_id"],
            (self._remote_future_id,),
            {},
        )

    @property
    def exception(self) -> Optional[dict]:
        """Return a serializable representation of the remote exception."""
        return self._comm._send_rpc(
            self._remote_policy_id,
            ["_get_future_exception"],
            (self._remote_future_id,),
            {},
        )

    def wait(self, timeout: Optional[float] = None) -> Any:
        """Block until the remote future completes, returning the result id."""
        return self._comm._send_rpc(
            self._remote_policy_id,
            ["_wait_future"],
            (self._remote_future_id,),
            {"timeout": timeout},
        )

    def __repr__(self) -> str:
        kind = "RemoteGroupFuture" if self._is_group else "RemoteFuture"
        return f"{kind}({self._remote_future_id!r}, policy={self._remote_policy_id!r})"
