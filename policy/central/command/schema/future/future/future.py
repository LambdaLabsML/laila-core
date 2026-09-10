"""Abstract :class:`Future` base class -- identity, status, callbacks, lifecycle.

A :class:`Future` is the laila-side analogue of
:class:`concurrent.futures.Future` / :class:`asyncio.Future`, but
extended with three things the standard library doesn't provide:

- **Identity.** A future has a stable ``global_id`` (UUID + scope)
  so it can be referenced across processes (see
  :class:`RemoteFuture`) and looked up in the owning policy's
  ``future_bank`` after the local handle has gone out of scope.
- **Result-as-Entry.** Reading ``future.result`` always yields an
  :class:`Entry`: non-Entry values set by the producer are wrapped in
  :meth:`Entry.constant` lazily on first read, so every future
  ultimately resolves to an addressable entry that can be
  ``laila.remember``'d on a peer without the runner paying for the
  wrap up front.
- **Explicit release.** Every future is registered in the owning
  policy's ``future_bank`` and stays there until :meth:`Future.release`
  is called -- nothing evicts futures automatically.
- **Status callbacks.** Multiple callbacks can be registered against
  any :class:`FutureStatus` transition via :meth:`add_status_callback`;
  late registrations on already-fired statuses fire immediately to
  close the obvious race.

Concrete subclasses provide the actual ``wait`` / ``__await__`` /
producer-side completion logic:

- :class:`ConcurrentPackageFuture` -- backed by a
  ``concurrent.futures.Future`` from a thread or process pool.
- :class:`ComplexFuture` -- chains of dependent futures; deprecated
  in favor of pure async coroutines on the async-thread-pool taskforce.
- :class:`GroupFuture` -- aggregates multiple child futures.
- :class:`RemoteFuture` -- proxies a future that lives on a peer.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from pydantic import ConfigDict, Field, PrivateAttr

from .......utils.decorators.synchronized import synchronized
from .future_identity import _LAILA_IDENTIFIABLE_FUTURE
from .future_status import FutureStatus


def _make_status_setter(status: FutureStatus) -> Callable[[Any], None]:
    def _set(f: Any) -> None:
        f.status = status

    _set.__name__ = f"_set_status_{status.name.lower()}"
    return _set


_DEFAULT_STATUS_CALLBACKS: dict[FutureStatus, Callable[[Any], None]] = {
    st: _make_status_setter(st) for st in FutureStatus
}
"""Shared "set my status to X" table used by every :class:`Future`.

Stateless, so one dict serves all instances -- building seven closures
per future was measurable on the per-task hot path.
"""


class Future(_LAILA_IDENTIFIABLE_FUTURE):
    """Abstract base class for all in-process laila futures.

    Inherits identity (``taskforce_id``, ``policy_id``,
    ``future_group_id``, ``precedence``, ``purpose`` and the usual
    ``uuid`` / ``scopes``) from :class:`_LAILA_IDENTIFIABLE_FUTURE`,
    and adds:

    - ``status`` (:class:`FutureStatus`) -- the lifecycle marker.
    - ``result`` -- the (possibly auto-wrapped) :class:`Entry` outcome.
    - ``exception`` -- the failure outcome.
    - ``_result_global_id`` -- the gid of the result entry (handy for
      cross-policy remembering).
    - ``callbacks`` / ``_status_callbacks`` -- per-status hook lists.

    On construction the future self-registers into the active local
    policy's ``future_bank`` and into every open guarantee scope on
    the calling thread. That is what makes ``with laila.guarantee:``
    work without any explicit registration on the caller's part.

    Subclasses must override :meth:`wait` (and typically ``__await__``)
    to hook into their concrete completion mechanism.
    """

    _status: FutureStatus = PrivateAttr(default=FutureStatus.NOT_STARTED)
    _return_value: Any = PrivateAttr(default=None)
    _exception: Exception | None = PrivateAttr(default=None)
    _result_global_id: str | None = PrivateAttr(default=None)
    _result_pending_wrap: bool = PrivateAttr(default=False)
    _timeout_ms: int = PrivateAttr(default=100)

    model_config = ConfigDict(arbitrary_types_allowed=True)

    # ``_default_callbacks`` is shared and immutable (see
    # ``_DEFAULT_STATUS_CALLBACKS``); ``_status_callbacks`` is created in
    # ``model_post_init``. Neither uses ``PrivateAttr(default_factory=...)``
    # because pydantic re-inspects a private factory's signature on every
    # instantiation -- futures are constructed on the per-task hot path.
    _default_callbacks: dict[FutureStatus, Callable[..., Any]] = PrivateAttr(default=None)
    _status_callbacks: dict[FutureStatus, list[Callable[..., Any]]] = PrivateAttr(default=None)
    callbacks: dict[FutureStatus, Callable[..., Any]] = Field(default_factory=dict)

    def model_post_init(self, __context: Any) -> None:
        """Apply staged identity, wire default per-status callbacks and
        self-register with the active local policy.

        Performs four things:

        1. Chain to :meth:`_LAILA_IDENTIFIABLE_OBJECT.model_post_init`
           so ``self.global_id`` reflects any explicit ``uuid=``.
        2. Point :attr:`_default_callbacks` at the shared
           "set my status to X when callback X fires" table.
        3. Resolve the active *local* policy (``_get_active_local_policy``
           lazily activates a ``DefaultPolicy`` if needed).
        4. Register ``self`` in every currently-open guarantee scope
           on this thread, then drop a reference into
           ``policy.future_bank`` so the future can be looked up by
           gid even after the local handle goes out of scope.

        Logging the creation event is best-effort -- failures here
        never block construction.
        """
        super().model_post_init(__context)
        self._setup_default_callbacks()
        from ....... import _get_active_local_policy

        policy = _get_active_local_policy()
        policy.central.command._register_future_with_active_guarantees(self)
        policy.future_bank[self.global_id] = self
        try:
            from .......logger import get_logger

            get_logger().record_future_created(self)
        except Exception:
            pass

    def _setup_default_callbacks(self) -> None:
        """Attach the shared default status-transition table and an empty
        per-instance status-callback registry."""
        self._default_callbacks = _DEFAULT_STATUS_CALLBACKS
        self._status_callbacks = {}

    @property
    @synchronized
    def status(self) -> FutureStatus:
        """
        Return the current status code for this Future.
        """
        return self._status

    @status.setter
    @synchronized
    def status(self, status: FutureStatus) -> None:
        """
        Set the current status code for this Future.

        After updating the internal state and emitting the standard logger
        transition, fires every callback registered for *status* via
        :meth:`add_status_callback`. Callback exceptions are swallowed to
        avoid disrupting the producer (taskforce runner) thread.
        """
        prev = self._status
        self._status = status
        if prev != status:
            try:
                from .......logger import get_logger

                get_logger().record_future_transition(self, status, prev)
            except Exception:
                pass
            cbs = list(self._status_callbacks.get(status, ()))
            for cb in cbs:
                try:
                    cb(self)
                except Exception:
                    pass

    def add_status_callback(self, status: FutureStatus, fn: Callable[[Future], Any]) -> None:
        """Register *fn* to fire when this future transitions into *status*.

        Multiple callbacks per status are supported (registered in insertion
        order, fired in insertion order). The callback runs on whatever
        thread set the status; it must therefore be cheap and non-blocking.
        Exceptions raised inside *fn* are swallowed.

        If the future is *already* in *status* at registration time, *fn* is
        invoked immediately and synchronously to close the obvious race
        between registration and a producer that completed first.
        """
        bucket = self._status_callbacks.setdefault(status, [])
        bucket.append(fn)
        if self._status == status:
            try:
                fn(self)
            except Exception:
                pass

    @property
    def result(self) -> Any:
        """The future's result, blocking until completion if necessary.

        Behavior:

        - If the future has already finished successfully, returns the
          recorded result without blocking.
        - If the future ended in error or was cancelled, the recorded
          exception is re-raised.
        - Otherwise, the lock is released and we ``wait(timeout=None)``
          for completion -- releasing the lock first is critical
          because the producer thread that ultimately sets the result
          will need to re-acquire the same atomic lock to do so.
        """
        with self.atomic():
            if self._status in [FutureStatus.ERROR, FutureStatus.CANCELLED]:
                raise self._exception
            if self._status == FutureStatus.FINISHED:
                return self._materialize_result()
        self.wait(timeout=None)
        with self.atomic():
            if self._status in [FutureStatus.ERROR, FutureStatus.CANCELLED]:
                raise self._exception
            return self._materialize_result()

    @result.setter
    @synchronized
    def result(self, result: Any) -> None:
        """Record the future's result; non-Entry values are wrapped lazily.

        - ``None`` clears both the value and the result-id slot.
        - A live :class:`Entry` is stored as-is and its ``global_id``
          is captured into ``_result_global_id`` immediately.
        - Anything else is stored raw and wrapped via
          :meth:`Entry.constant` on first read (``result`` / ``data`` /
          :attr:`result_global_id`), so the producer's hot path does not
          pay for an entry nobody may ever look at. The outcome is still
          always addressable across processes once read.
        """
        from .......entry import Entry

        if result is None:
            self._return_value = None
            self._result_global_id = None
            self._result_pending_wrap = False
        elif isinstance(result, Entry):
            self._return_value = result
            self._result_global_id = result.global_id
            self._result_pending_wrap = False
        else:
            self._return_value = result
            self._result_global_id = None
            self._result_pending_wrap = True

    def _materialize_result(self) -> Any:
        """Return the result as an :class:`Entry`, wrapping a raw value on first use.

        Must be called with the atomic lock held (it is re-entrant, so
        callers already inside ``with self.atomic()`` are fine).
        """
        with self.atomic():
            if self._result_pending_wrap:
                from .......entry import Entry

                wrapped = Entry.constant(data=self._return_value)
                self._return_value = wrapped
                self._result_global_id = wrapped.global_id
                self._result_pending_wrap = False
            return self._return_value

    @property
    def result_global_id(self) -> str | None:
        """``global_id`` of the result entry, or ``None`` if there is no result yet.

        Forces the lazy :meth:`Entry.constant` wrap for raw results so the
        id is stable from the first time anyone asks for it. Never blocks.
        """
        with self.atomic():
            if self._result_pending_wrap:
                self._materialize_result()
            return self._result_global_id

    @property
    def data(self) -> Any:
        """Return ``self.result.data`` -- the unwrapped payload value.

        Convenience accessor for the common case of "I just want the
        Python value the task produced". Blocks until the future
        finishes (via the underlying ``result`` getter) and then
        unwraps the entry.

        Raises
        ------
        RuntimeError
            If the result is not an :class:`Entry` instance (which can
            happen for futures that explicitly set ``None`` results,
            or for raw non-Entry results from legacy code paths).
        """
        from .......entry import Entry

        result = self.result
        if not isinstance(result, Entry):
            raise RuntimeError(
                f"Future result is not an Entry (got {type(result).__name__}); cannot access .data"
            )
        return result.data

    @property
    def exception(self) -> Exception | None:
        """
        Return the current exception value.
        """
        return self._exception

    @exception.setter
    @synchronized
    def exception(self, exception: Exception | None) -> None:
        """
        Set the exception value.
        """
        self._exception = exception

    def add_callback(self, status: FutureStatus, fn: Callable[[Future], Any]) -> None:
        """
        Register a callback for a specific status transition.
        """
        self.callbacks[status] = fn

    def remove_callback(self, status: FutureStatus, fn: Callable[[Future], Any]) -> None:
        """
        Remove a callback for a specific status.
        """
        self.callbacks[status] = None

    def clear_callbacks(self, status: FutureStatus) -> None:
        """
        Clear the callback for a specific status.
        """
        self._callbacks[status] = None

    def clear_all_callbacks(self) -> None:
        """
        Clear all registered callbacks.
        """
        self._callbacks.clear()

    # TODO: This needs to go through the central command.
    def trigger_callback(self, status: FutureStatus) -> None:
        """
        Trigger the callback for the given status, if present.
        """
        fn = self._callbacks[status]
        if fn is not None:
            fn(self.result)

    @property
    def future_identity(self) -> _LAILA_IDENTIFIABLE_FUTURE:
        """Return a lightweight identity handle for this future."""
        return _LAILA_IDENTIFIABLE_FUTURE(
            taskforce_id=self.taskforce_id,
            policy_id=self.policy_id,
            future_group_id=self.future_group_id,
            precedence=self.precedence,
            purpose=self.purpose,
            uuid=self._uuid,
        )

    def release(self) -> None:
        """Remove this future from its owning policy's ``future_bank``.

        The bank holds a strong reference to every future (and therefore
        to its result payload) until this is called; nothing releases a
        future automatically. Call it once the result has been consumed
        and drop your own handle to let the object be collected.

        Idempotent and legal in any state: a still-running task keeps
        its own reference to the future, so releasing early only removes
        the gid-based lookup (``laila.runtime.status(gid)``,
        :class:`RemoteFuture` RPCs, identity handles) -- completion is
        unaffected. Open guarantee scopes are left untouched; they are
        transient and drained on exit.
        """
        from ....... import _local_policies

        gid = self.global_id
        pid = self.policy_id.global_id if hasattr(self.policy_id, "global_id") else self.policy_id
        policy = _local_policies.get(pid) if isinstance(pid, str) else None
        if policy is not None and policy.future_bank.get(gid) is self:
            policy.future_bank.pop(gid, None)
            return
        for policy in _local_policies.values():
            if policy.future_bank.get(gid) is self:
                policy.future_bank.pop(gid, None)
                return

    def wait(self, timeout: float | None = None) -> Any:
        """Block until the future completes. Subclasses must override.

        Concrete subclasses must implement this and additionally guard
        against being invoked from an async loop thread (call
        :func:`_check_not_loop_thread` first) -- waiting synchronously
        on a future from within the same loop that is supposed to
        complete it is a hang.

        Raises
        ------
        NotImplementedError
            Always, on the abstract base.
        """
        raise NotImplementedError(
            f"{type(self).__name__} does not implement wait(); "
            "use a concrete subclass such as ConcurrentPackageFuture."
        )

    def finished(self) -> bool:
        """
        Return True if the Future has finished successfully.
        """
        return self.status in [FutureStatus.FINISHED]

    def cancelled(self) -> bool:
        """
        Return True if the Future was cancelled.
        """
        return self.status in [FutureStatus.CANCELLED]

    def error(self) -> bool:
        """
        Return True if the Future finished with an error.
        """
        return self.status in [FutureStatus.ERROR]

    def not_started(self) -> bool:
        """
        Return True if the Future has not started.
        """
        return self.status in [FutureStatus.NOT_STARTED]

    def running(self) -> bool:
        """
        Return True if the Future is currently running.
        """
        return self.status in [FutureStatus.RUNNING]
