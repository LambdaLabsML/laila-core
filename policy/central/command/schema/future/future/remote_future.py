"""Pydantic-backed proxy for a future that lives on a remote policy.

:class:`RemoteFuture` is the client-side handle returned whenever an
RPC call on a peer yields a serialized future payload (the peer's
:class:`Future` ``model_dump`` includes a ``__laila_future__`` marker
that :meth:`Communication._maybe_wrap_remote_future` rehydrates into
a :class:`RemoteFuture` on this side).

Why it inherits from :class:`_LAILA_IDENTIFIABLE_FUTURE`
--------------------------------------------------------
The identity base class gives a remote future the *same* gid /
``future_bank`` / guarantee-scope behaviour as a local future, so
existing helpers (``laila.runtime.wait``, ``with laila.guarantee:``,
remote-fetch chains) can treat both flavors uniformly. Only the
status / result / exception / wait accessors override the base to
forward over the owning :class:`Communication` channel instead of
reading local state.

Result handling
---------------
``RemoteFuture.result`` returns the *global_id* of the result entry
on the peer rather than the entry itself -- the caller typically
hands that gid to :func:`laila.remember` to materialize the payload
through a shared pool. Returning the gid keeps RPC frames small and
defers the (potentially expensive) payload transfer to the explicit
remember.
"""

from __future__ import annotations

import asyncio
from typing import Any, Optional, TYPE_CHECKING

from pydantic import ConfigDict, PrivateAttr

from .future_identity import _LAILA_IDENTIFIABLE_FUTURE
from .future_status import FutureStatus

if TYPE_CHECKING:
    from ....communication.schema.base import _LAILA_IDENTIFIABLE_COMMUNICATION


class RemoteFuture(_LAILA_IDENTIFIABLE_FUTURE):
    """Proxy for a future that lives on a remote policy's future bank.

    The local instance carries only identity data plus a back-reference to
    the communication channel that serves the owning peer; every status
    or result query is forwarded over that channel.

    Parameters
    ----------
    taskforce_id : str
        ``global_id`` of the taskforce on the remote side (falls back to
        ``peer_id`` when the remote payload omits it).
    policy_id : str
        ``global_id`` of the remote policy that owns the future.
    uuid : str
        The trailing uuid segment of the remote future's ``global_id`` so
        the local identity's ``global_id`` matches its counterpart on the
        peer.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    _comm: Any = PrivateAttr(default=None)
    _is_group: bool = PrivateAttr(default=False)

    def bind(
        self,
        communication: "_LAILA_IDENTIFIABLE_COMMUNICATION",
        *,
        is_group: bool = False,
    ) -> None:
        """Attach the communication channel and group-flag after construction.

        Called by :meth:`Communication._maybe_wrap_remote_future` once
        the proxy has been built. Uses ``object.__setattr__`` to bypass
        Pydantic's validation (the channel reference is a
        :class:`PrivateAttr` so this is safe).
        """
        object.__setattr__(self, "_comm", communication)
        object.__setattr__(self, "_is_group", is_group)

    def model_post_init(self, __context: Any) -> None:
        """Apply staged identity fields and self-register with the active local policy.

        Mirrors :meth:`Future.model_post_init` so a remote future
        participates in guarantee scopes and the future bank just like
        a local one. The communication channel is bound separately via
        :meth:`bind` after construction completes.
        """
        super().model_post_init(__context)
        from ....... import _get_active_local_policy

        policy = _get_active_local_policy()
        policy.central.command._register_future_with_active_guarantees(self)
        policy.future_bank[self.global_id] = self

    @property
    def is_group(self) -> bool:
        """Return ``True`` when the remote future is a ``GroupFuture``."""
        return self._is_group

    @property
    def status(self) -> FutureStatus:
        """Query the remote policy for this future's current status."""
        raw = self._comm._send_rpc(
            str(self.policy_id),
            ["_get_future_status"],
            (self.global_id,),
            {},
        )
        if isinstance(raw, FutureStatus):
            return raw
        if isinstance(raw, str):
            try:
                return FutureStatus(raw)
            except ValueError:
                try:
                    return FutureStatus[raw]
                except KeyError:
                    return raw
        return raw

    @property
    def result(self) -> Any:
        """Return the ``global_id`` of the result entry on the remote side.

        Callers typically pass this into ``laila.remember(result_gid, ...)``
        to fetch the actual payload through a shared pool.
        """
        return self._comm._send_rpc(
            str(self.policy_id),
            ["_get_future_result_id"],
            (self.global_id,),
            {},
        )

    @property
    def exception(self) -> Optional[dict]:
        """Return a serialized representation of the remote exception, if any."""
        return self._comm._send_rpc(
            str(self.policy_id),
            ["_get_future_exception"],
            (self.global_id,),
            {},
        )

    def wait(self, timeout: Optional[float] = None) -> Any:
        """Block until the remote future completes, returning the result id.

        Raises
        ------
        LoopBlockingWaitError
            If called from a thread that owns an async event loop.
        """
        from ...exceptions import _check_not_loop_thread
        _check_not_loop_thread()

        return self._comm._send_rpc(
            str(self.policy_id),
            ["_wait_future"],
            (self.global_id,),
            {"timeout": timeout},
        )

    def __await__(self):
        """Await the remote future without blocking the calling event loop.

        The blocking RPC :meth:`wait` is offloaded to a worker thread
        via :func:`asyncio.to_thread`, so the event loop stays free to
        service other coroutines while the wait is in flight.
        """

        async def _run():
            return await asyncio.to_thread(self.wait, None)

        return _run().__await__()

    def __repr__(self) -> str:
        """Return a short human-readable representation."""
        kind = "RemoteGroupFuture" if self._is_group else "RemoteFuture"
        return f"{kind}({self.global_id!r}, policy={self.policy_id!r})"
