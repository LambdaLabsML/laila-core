"""Abstract base task-force with lifecycle management and a submission queue.

A *task-force* is laila's name for a worker pool. Concrete subclasses
(``PythonAsyncThreadPoolTaskForce``, ``PythonThreadPoolTaskForce``,
``PythonProcessPoolTaskForce``) implement the actual scheduling -- this
base just nails down:

- a uniform :class:`TaskForceStatus` lifecycle
  (NOT_STARTED -> RUNNING -> [PAUSED] -> STOPPED);
- a process-wide registry of *live* taskforces (used by
  :func:`laila.terminate` to sweep orphans -- those not reachable
  through any policy);
- a uniform :meth:`submit` / :meth:`imap` contract (subclass-defined);
- context-manager sugar so a task-force can be used as ``with tf: ...``;
- a ``_q`` :class:`AtomicDict` slot subclasses can repurpose as a
  per-task-force submission queue.

Lifecycle invariants
--------------------
- ``model_post_init`` calls :meth:`start` for any taskforce constructed
  in the ``NOT_STARTED`` state, so freshly-instantiated taskforces are
  immediately usable.
- :meth:`start` and :meth:`shutdown` only flip the status flag *after*
  the subclass-specific hook (``_on_start`` / ``_on_shutdown``)
  returns, so partial transitions cannot leave a taskforce in an
  inconsistent state.
"""

from __future__ import annotations

import threading
from collections.abc import Callable, Iterable
from typing import Any

from pydantic import ConfigDict, PrivateAttr

from .....atomic import AtomicDict
from .....atomic.definitions.locally_atomic_identifiable_object import (
    _LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT,
)
from .....basics.definitions.cli_capable import _LAILA_CLI_CAPABLE_CLASS, CLIExempt
from .....basics.definitions.identifiable_object import _LAILA_IDENTIFIABLE_OBJECT
from .....macros.strings import _TASK_FORCE_SCOPE
from ..schema.future.future import Future
from .status import TaskForceStatus

_LIVE_TASKFORCES: set[Any] = set()
_LIVE_TASKFORCES_LOCK = threading.Lock()


def _register_live_taskforce(tf: Any) -> None:
    """Track *tf* in the process-wide live set.

    The set is consumed by :func:`laila.terminate` to sweep "orphan"
    task-forces -- those constructed directly (e.g. ``TaskForce.process_pool()``
    in user code) and never attached to a policy. Without this set
    those would leak their worker threads / processes through to
    interpreter shutdown.

    Thread-safe via :data:`_LIVE_TASKFORCES_LOCK`.
    """
    with _LIVE_TASKFORCES_LOCK:
        _LIVE_TASKFORCES.add(tf)


def _unregister_live_taskforce(tf: Any) -> None:
    """Remove *tf* from the live set. Best-effort; idempotent.

    Called during :meth:`_LAILA_IDENTIFIABLE_TASK_FORCE.shutdown`.
    """
    with _LIVE_TASKFORCES_LOCK:
        _LIVE_TASKFORCES.discard(tf)


def _live_taskforces_snapshot() -> list[Any]:
    """Return a snapshot list of currently-registered live task-forces.

    The snapshot is detached from the live set, so callers can iterate
    without holding the registry lock and without worrying about
    concurrent mutation.
    """
    with _LIVE_TASKFORCES_LOCK:
        return list(_LIVE_TASKFORCES)


class _LAILA_IDENTIFIABLE_TASK_FORCE(
    _LAILA_CLI_CAPABLE_CLASS, _LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT
):
    """Abstract base for task-forces -- worker-pool registries with a uniform
    submit / shutdown contract.

    Concrete subclasses provide the actual scheduling backend (asyncio
    thread pool, classic ``ThreadPoolExecutor``, ``ProcessPoolExecutor``,
    or remote pools). The base class enforces a single status state
    machine and queue-length introspection so callers can write
    backend-agnostic code.
    """

    _scopes: list[str] = PrivateAttr(default_factory=lambda: list([_TASK_FORCE_SCOPE]))

    model_config = ConfigDict(arbitrary_types_allowed=True)

    policy_id: _LAILA_IDENTIFIABLE_OBJECT | str | None = CLIExempt(default=None)

    status: TaskForceStatus = CLIExempt(
        default=TaskForceStatus.NOT_STARTED,
        description="Current lifecycle status of this TaskForce.",
    )

    # ---- Shared runtime state (available to subclasses) ----
    _q: AtomicDict[str, tuple[Callable[..., Any], tuple, dict]] = PrivateAttr(
        default_factory=AtomicDict
    )

    def model_post_init(self, __context: Any) -> None:
        """Auto-start the task-force when constructed in ``NOT_STARTED``.

        This makes ``MyTaskForce()`` immediately usable -- callers
        rarely need to remember a separate ``.start()`` call. To
        construct without starting, pass ``status=TaskForceStatus.PAUSED``
        explicitly.
        """
        if self.status == TaskForceStatus.NOT_STARTED:
            self.start()

    # ---------- Observability ----------
    @property
    def queue_len(self) -> int:
        """Number of submitted tasks not yet observed as completed.

        Backed by the per-instance :class:`AtomicDict` queue ``_q``;
        thread-safe by virtue of the underlying atomic-dict locking.
        """
        return len(self._q)

    def __len__(self) -> int:
        """``len(taskforce)`` returns the current queue length, not the worker count.

        Defined this way so ``if not tf:`` reads naturally as "the
        taskforce has nothing pending".
        """
        return int(self.queue_len)

    # ---------- Public lifecycle ----------
    def start(self) -> None:
        """Start the task-force.

        Idempotent: a second call on a running task-force is a no-op.
        Calls the subclass-defined :meth:`_on_start` hook first; only
        flips the status to ``RUNNING`` if the hook returns without
        raising. The taskforce is also added to the process-wide
        live registry here so :func:`laila.terminate` can find it.
        """
        if self.status == TaskForceStatus.RUNNING:
            return
        self._on_start()
        self.status = TaskForceStatus.RUNNING
        _register_live_taskforce(self)

    def pause(self) -> None:
        """Quiesce the task-force without destroying its resources.

        Calls :meth:`_on_pause` and flips the status to ``PAUSED`` --
        future submissions are still accepted by some backends but
        not actively dispatched until :meth:`start` is called again.
        No-op when the task-force is not currently ``RUNNING``.
        """
        if self.status != TaskForceStatus.RUNNING:
            return
        self._on_pause()
        self.status = TaskForceStatus.PAUSED

    def shutdown(self, wait: bool = True, cancel_pending: bool = False) -> None:
        """Tear down the task-force and release its resources.

        Idempotent: a second call on a stopped task-force still
        unregisters it from the live set (cheap) and returns. The
        subclass-defined :meth:`_on_shutdown` hook runs inside a
        ``try`` so the status flag is *always* updated to ``STOPPED``
        and the live-set registration removed, even if the hook raises.

        Parameters
        ----------
        wait : bool, default True
            Block until in-flight tasks finish before returning.
        cancel_pending : bool, default False
            Drop tasks that have been queued but not yet started.
        """
        if self.status == TaskForceStatus.STOPPED:
            _unregister_live_taskforce(self)
            return
        try:
            self._on_shutdown(wait=wait, cancel_pending=cancel_pending)
        finally:
            self.status = TaskForceStatus.STOPPED
            _unregister_live_taskforce(self)

    # Context manager sugar
    def __enter__(self):
        """Enter context: ensure the task-force is running and return ``self``."""
        self.start()
        return self

    def __exit__(self, exc_type, exc, tb):
        """Exit context: shut down with ``wait=True`` regardless of exception state.

        Pending tasks are *not* cancelled by default, matching the
        executor convention from the standard library. Pass to
        :meth:`shutdown` directly if you need different semantics.
        """
        self.shutdown(wait=True)

    # ---------- Subclass hooks (must implement) ----------
    def _on_start(self) -> None:
        """Allocate or start backend resources (worker threads, dispatchers, ...).

        Called from :meth:`start` *before* the status flag flips to
        ``RUNNING``. If this raises, the task-force stays in its
        previous state and is *not* added to the live registry.
        """
        raise NotImplementedError

    def _on_pause(self) -> None:
        """Quiesce backend resources without tearing them down.

        Called from :meth:`pause`. The default base implementation
        raises :class:`NotImplementedError`; backends that cannot
        meaningfully pause should still implement it as a no-op so
        ``pause()`` is at least safe to call.
        """
        raise NotImplementedError

    def _on_shutdown(self, *, wait: bool, cancel_pending: bool) -> None:
        """Tear down backend resources, honouring *wait* and *cancel_pending*.

        Called from :meth:`shutdown` inside a ``try``: any exception
        raised here is allowed to propagate, but the status flag and
        live-set registration are still cleaned up by the caller.
        """
        raise NotImplementedError

    # ---------- Mapping / submit API (must implement) ----------
    def submit(
        self,
        funcs: Iterable[Callable[[], Any]],
        wait: bool = False,
    ) -> Future | list[Any] | Any:
        """Batch-submit an iterable of zero-arg callables.

        Subclasses must accept *funcs* of any length and obey the
        return-shape contract below so callers can write
        backend-agnostic code.

        Parameters
        ----------
        funcs : Iterable[Callable[[], Any]]
            Zero-arg callables (sync or coroutine functions, depending
            on backend) to enqueue.
        wait : bool, default False
            If True, block until all submissions complete and return
            the result(s) directly. If False, return future(s).

        Returns
        -------
        Future | list | Any
            ============   ===============   =====================
            ``len(funcs)`` ``wait``          Return type
            ============   ===============   =====================
            ``1``          ``False``         A single :class:`Future`
            ``> 1``        ``False``         A grouped/aggregate :class:`Future` (e.g. :class:`GroupFuture`)
            ``1``          ``True``          The single result value
            ``> 1``        ``True``          A list of result values
            ============   ===============   =====================
        """
        raise NotImplementedError

    def imap(self, funcs: Iterable[Callable[[], Any]]) -> Iterable[Future]:
        """Submit *funcs* lazily, yielding :class:`Future` instances in submission order.

        Unlike :meth:`submit`, this is an iterator: each ``next(...)``
        triggers the next submission. Useful for streaming pipelines
        where the iterable is very large or even unbounded.

        The yielded futures complete in arbitrary order; pair with
        :meth:`Future.wait` or ``await`` if you need the results.
        """
        raise NotImplementedError
