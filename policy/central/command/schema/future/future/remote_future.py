"""Pydantic-backed proxy for a future that lives on a remote policy.

``RemoteFuture`` is the client-side handle returned whenever an RPC
call on a peer yields a serialized future payload.  It inherits the
identity fields (``global_id``, ``policy_id``, ``taskforce_id``, ...)
from :class:`_LAILA_IDENTIFIABLE_FUTURE` so the guarantee stack,
``future_bank``, and ``laila.runtime`` helpers can treat it like any
other Laila future; all concrete state queries (``status``, ``result``,
``exception``, ``wait``) transparently dispatch over the owning
:class:`_LAILA_IDENTIFIABLE_COMMUNICATION` channel.
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
        """Attach the communication channel and group flag after construction."""
        object.__setattr__(self, "_comm", communication)
        object.__setattr__(self, "_is_group", is_group)

    def model_post_init(self, __context: Any) -> None:
        """Apply staged identity fields and register with the local policy."""
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
        """Block until the remote future completes, returning the result id."""
        return self._comm._send_rpc(
            str(self.policy_id),
            ["_wait_future"],
            (self.global_id,),
            {"timeout": timeout},
        )

    def __await__(self):
        """Await the remote future without blocking the event loop."""

        async def _run():
            return await asyncio.to_thread(self.wait, None)

        return _run().__await__()

    def __repr__(self) -> str:
        """Return a short human-readable representation."""
        kind = "RemoteGroupFuture" if self._is_group else "RemoteFuture"
        return f"{kind}({self.global_id!r}, policy={self.policy_id!r})"
