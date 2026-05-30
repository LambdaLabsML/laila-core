"""Base schema for the central command sub-system.

Central command is the policy's submission hub. It owns:

- a registry of :class:`_LAILA_IDENTIFIABLE_TASK_FORCE` instances
  (keyed by gid) plus a designated ``alpha_taskforce`` that receives
  work when the caller does not specify a target;
- a thread-local *guarantee stack* used by ``with laila.guarantee:``
  to record every future created inside a scope so the scope can
  block on them all at exit.

The :meth:`submit` method is the single entry point for all queued
work. Sync callables submitted through it are auto-wrapped into
trivial coroutine functions so the runner sees a uniform contract --
no separate "sync vs async submit" plumbing is needed downstream.
"""

import threading
from collections.abc import Callable, Iterable
from typing import Any

from pydantic import PrivateAttr

from .....basics.definitions.cli_capable import _LAILA_CLI_CAPABLE_CLASS, CLIExempt
from .....basics.definitions.identifiable_object import _LAILA_IDENTIFIABLE_OBJECT
from .....macros.strings import _CENTRAL_COMMAND_SCOPE
from .exceptions import (
    _check_no_pending_submit_owner,
    ensure_coroutine_function,
)
from .future.future import Future


class _LAILA_IDENTIFIABLE_CENTRAL_COMMAND(_LAILA_CLI_CAPABLE_CLASS, _LAILA_IDENTIFIABLE_OBJECT):
    """Central command -- task-force registry, work submission, and guarantee scopes.

    Owns
    ----
    - ``taskforces`` : ``{gid: TaskForce}`` -- every registered taskforce.
    - ``alpha_taskforce`` : the gid of the default taskforce. Created
      automatically by :meth:`model_post_init` if no taskforces were
      provided.
    - ``policy_id`` : back-reference to the owning policy's gid.

    Provides
    --------
    - :meth:`submit` -- the universal submission API.
    - :meth:`shutdown` -- shuts down every registered taskforce.
    - The thread-local guarantee stack hooks
      (:meth:`_guarantee_enter`, :meth:`_guarantee_exit`,
      :meth:`_register_future_with_active_guarantees`) used by the
      ``laila.guarantee`` context manager.
    """

    _scopes: list[str] = PrivateAttr(default_factory=lambda: list([_CENTRAL_COMMAND_SCOPE]))

    taskforces: dict[str, Any] = CLIExempt(default_factory=dict)
    alpha_taskforce: str | None = None
    policy_id: _LAILA_IDENTIFIABLE_OBJECT | str | None = CLIExempt(default=None)
    _guarantee_local: threading.local = PrivateAttr(default_factory=threading.local)

    def model_post_init(self, __context: Any) -> None:
        """Create a default async taskforce if none was registered.

        When ``taskforces`` is empty, auto-creates a single
        ``DefaultTaskForce`` (a ``PythonAsyncThreadPoolTaskForce``) and
        points ``alpha_taskforce`` at it. There is no longer a built-in
        IO/compute role split — sync work submitted to the async taskforce
        runs inline on whatever loop thread the dispatcher picks; async
        work interleaves on loop slots. Users can still register
        additional taskforces (e.g. process pools) via ``add_taskforce``.
        """
        if len(self.taskforces) == 0:
            from .....macros.defaults import DefaultTaskForce

            tf = DefaultTaskForce(policy_id=self.policy_id)
            self.taskforces[tf.global_id] = tf
            self.alpha_taskforce = tf.global_id

        if self.alpha_taskforce is None or self.alpha_taskforce not in self.taskforces:
            self.alpha_taskforce = next(iter(self.taskforces))

        return self

    def add_taskforce(
        self,
        taskforce: Any,
    ):
        """Register a task-force with this central command.

        Subsequent :meth:`submit` calls can target it via
        ``taskforce_id=<gid>``. Re-registering the same gid silently
        overwrites the previous instance, which is sometimes useful
        when hot-swapping a taskforce implementation.

        Parameters
        ----------
        taskforce : _LAILA_IDENTIFIABLE_TASK_FORCE
            The task-force instance to register.
        """
        self.taskforces[taskforce.global_id] = taskforce

    def _guarantee_stack(self) -> list[dict[str, Future]]:
        """Return the thread-local stack of open guarantee scopes.

        The stack lives on a :class:`threading.local` so each thread
        has its own independent set of scopes -- nested
        ``with laila.guarantee:`` blocks in one thread don't see
        futures created on another. Created lazily on first access.
        """
        stack = getattr(self._guarantee_local, "stack", None)
        if stack is None:
            stack = []
            self._guarantee_local.stack = stack
        return stack

    def _guarantee_enter(self) -> None:
        """Push a fresh empty scope onto the calling thread's guarantee stack.

        Called by the ``laila.guarantee`` context manager on
        ``__enter__``. Subsequent futures created on this thread are
        registered into this scope by
        :meth:`_register_future_with_active_guarantees` until the
        scope is popped.
        """
        self._guarantee_stack().append({})

    def _guarantee_exit(self) -> list[Future]:
        """Pop the top guarantee scope and return its accumulated futures.

        Returns ``[]`` (rather than raising) when no scope is open, so
        that nested context-manager unwinding paths are robust to
        partial setup.
        """
        stack = self._guarantee_stack()
        if not stack:
            return []
        return list(stack.pop().values())

    def _register_future_with_active_guarantees(self, future: Any) -> None:
        """Record *future* in every guarantee scope open on this thread.

        Called by every :class:`Future` / :class:`GroupFuture` inside
        ``model_post_init`` so that any future created while
        ``with laila.guarantee:`` is in effect is automatically tracked
        for the scope's join-on-exit pass. No-op when no scopes are
        open (the common case).
        """
        stack = getattr(self._guarantee_local, "stack", None)
        if not stack:
            return

        for scope_futures in stack:
            scope_futures[future.global_id] = future

    def submit(
        self,
        tasks: Iterable[Callable[[], Any]],
        wait: bool = False,
        *,
        taskforce_id: str | None = None,
    ) -> Future | list[Any] | Any:
        """Submit zero-argument callables to a task-force for execution.

        Calling protocol
        ----------------
        - With one task and ``wait=False``: returns a single
          :class:`Future`.
        - With many tasks and ``wait=False``: returns a
          :class:`GroupFuture` aggregating per-task futures.
        - With ``wait=True``: blocks until all tasks finish, then
          returns the lone result (one task) or a list of results
          (many tasks).

        Sync callables are auto-wrapped into trivial coroutine
        functions at submission time so the runner sees a uniform
        awaitable contract. The wrapped sync body still runs inline
        on its loop thread (no implicit thread-pool offload).

        Reentrancy guard
        ----------------
        Calling :meth:`submit` from inside a function decorated
        ``@no_command_submit`` raises :exc:`NestedCommandSubmitError`.
        This guards against accidental "submit-inside-task" patterns
        that can deadlock the loop when the task itself was running on
        the same loop thread.

        Parameters
        ----------
        tasks : Iterable[Callable[[], Any]]
            Zero-arg callables. Sync and async callables are both
            accepted -- the wrapper does the right thing per task.
        wait : bool, default False
            If ``True``, block until all tasks complete and return
            their results synchronously.
        taskforce_id : str, optional
            Target task-force gid. Defaults to the alpha task-force.

        Returns
        -------
        Future or list[Any] or Any
            A future-like handle when ``wait=False``; a result or list
            of results when ``wait=True``.

        Raises
        ------
        NestedCommandSubmitError
            If invoked from inside a function decorated
            ``@no_command_submit``.
        KeyError
            If ``taskforce_id`` is supplied but not registered.
        """
        _check_no_pending_submit_owner()

        if taskforce_id is None:
            taskforce_id = self.alpha_taskforce

        wrapped = [ensure_coroutine_function(t) for t in tasks]

        return self.taskforces[taskforce_id].submit(
            tasks=wrapped,
            wait=wait,
        )

    def shutdown(self, wait: bool = True, cancel_pending: bool = False) -> None:
        """Shut down every registered task-force.

        Iterates over ``self.taskforces.values()`` and calls
        ``shutdown(wait, cancel_pending)`` on each. Order is iteration
        order (currently insertion order). Failures in one taskforce
        do NOT prevent the others from being shut down -- exceptions
        propagate after the iteration to the caller, but the
        :func:`laila.terminate` call site catches them so partial
        teardown still completes.

        Parameters
        ----------
        wait : bool, default True
            Block until workers finish in-flight tasks.
        cancel_pending : bool, default False
            Drop tasks that have been queued but not yet started.
            Already-running tasks are not cancelled regardless of
            this flag.
        """
        for tf in self.taskforces.values():
            tf.shutdown(wait=wait, cancel_pending=cancel_pending)

    def __await__(self):
        """Async awaiting is not yet supported."""
        raise NotImplementedError
