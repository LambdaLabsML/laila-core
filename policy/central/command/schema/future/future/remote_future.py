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
``RemoteFuture`` fully mirrors a local :class:`Future`: ``.result`` /
``.wait()`` block until the peer's future completes, transfer the result
entry's bytes over the wire (serialized with ``transformation_base64``
on the peer, rebuilt via ``build_by_scope`` here), and return the real
:class:`Entry`; ``.data`` returns its payload. The materialized result
is cached so repeated access never re-transfers. ``.status`` /
``.exception`` remain lightweight proxies that do not move the payload.
The cheap result-gid pointer is still available on the peer via
``_get_future_result_id`` for callers that want it.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

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
    _comm_selector: Any = PrivateAttr(default=None)
    _materialized: Any = PrivateAttr(default=None)
    _materialized_set: bool = PrivateAttr(default=False)

    def bind(
        self,
        communication: _LAILA_IDENTIFIABLE_COMMUNICATION,
        *,
        is_group: bool = False,
        comm: Any = None,
    ) -> None:
        """Attach the communication channel and group-flag after construction.

        Called by :meth:`Communication._maybe_wrap_remote_future` once
        the proxy has been built. Uses ``object.__setattr__`` to bypass
        Pydantic's validation (the channel reference is a
        :class:`PrivateAttr` so this is safe).

        The *comm* selector (a communication id / protocol token, or
        ``None``) is remembered so every follow-up ``status`` / ``wait``
        / ``result`` call stays on the same transport that produced the
        future.
        """
        object.__setattr__(self, "_comm", communication)
        object.__setattr__(self, "_is_group", is_group)
        object.__setattr__(self, "_comm_selector", comm)

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
            comm=self._comm_selector,
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

    def _rebuild(self, blob: Any) -> Any:
        """Rebuild a live Entry (or list for a group) from a wire blob."""
        from .......entry.constitution.build_maps import build_by_scope

        if self._is_group:
            blob = blob or []
            return [
                (build_by_scope(b, asynchronous=False) if b is not None else None) for b in blob
            ]
        if blob is None:
            return None
        return build_by_scope(blob, asynchronous=False)

    @property
    def result(self) -> Any:
        """Return the rebuilt result :class:`Entry` from the remote future.

        Mirrors a local ``Future.result``: blocks until the peer's future
        completes, transfers the entry over the wire, and returns the
        rebuilt :class:`Entry` (a list for a group future). Cached so a
        second access does not re-transfer.
        """
        if self._materialized_set:
            return self._materialized
        blob = self._comm._send_rpc(
            str(self.policy_id),
            ["_get_future_result_entry"],
            (self.global_id,),
            {},
            comm=self._comm_selector,
        )
        entry = self._rebuild(blob)
        object.__setattr__(self, "_materialized", entry)
        object.__setattr__(self, "_materialized_set", True)
        return entry

    @property
    def data(self) -> Any:
        """Return the payload of the result entry (mirrors local ``Future.data``).

        Blocking: materializes the entry from the peer if needed, then
        unwraps its ``data`` (a list of payloads for a group future).
        """
        result = self.result
        if self._is_group:
            return [(e.data if e is not None else None) for e in result]
        return result.data if result is not None else None

    @property
    def result_id(self) -> Any:
        """Return just the result entry's ``global_id`` (cheap pointer, no payload)."""
        return self._comm._send_rpc(
            str(self.policy_id),
            ["_get_future_result_id"],
            (self.global_id,),
            {},
            comm=self._comm_selector,
        )

    @property
    def exception(self) -> dict | None:
        """Return a serialized representation of the remote exception, if any."""
        return self._comm._send_rpc(
            str(self.policy_id),
            ["_get_future_exception"],
            (self.global_id,),
            {},
            comm=self._comm_selector,
        )

    def wait(self, timeout: float | None = None) -> Any:
        """Block until the remote future completes; return the rebuilt Entry.

        Mirrors a local ``Future.wait``: transfers the result entry over
        the wire and returns the rebuilt :class:`Entry` (cached).

        Raises
        ------
        LoopBlockingWaitError
            If called from a thread that owns an async event loop.
        """
        from ...exceptions import _check_not_loop_thread

        _check_not_loop_thread()

        if self._materialized_set:
            return self._materialized
        blob = self._comm._send_rpc(
            str(self.policy_id),
            ["_wait_future_entry"],
            (self.global_id,),
            {"timeout": timeout},
            comm=self._comm_selector,
        )
        entry = self._rebuild(blob)
        object.__setattr__(self, "_materialized", entry)
        object.__setattr__(self, "_materialized_set", True)
        return entry

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
