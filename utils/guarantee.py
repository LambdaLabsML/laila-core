"""Guarantee context managers that block until tracked futures complete.

The "guarantee" pattern is laila's lexical answer to "I want to be
sure every async operation kicked off in *this block* has finished
before the block returns". Two mirror implementations live here:

- :class:`_Guarantee` -- synchronous version. Use as
  ``with laila.guarantee: ...``. On exit, every future created
  *inside* the block is awaited via :meth:`Future.wait`. The first
  exception observed is re-raised so the caller never silently
  outlives a failed background task.
- :class:`_AsyncGuarantee` -- asynchronous version. Use as
  ``async with laila.guarantee_async: ...``. Same idea, but with
  ``await`` instead of ``wait``, plus a background watcher task
  that *immediately* cancels the parent task if any in-scope
  future errors -- so a hanging coroutine does not have to wait
  for ``__aexit__`` to learn about a failure.

Both classes co-operate with :class:`_LAILA_IDENTIFIABLE_CENTRAL_COMMAND`'s
``_guarantee_stack`` / ``_guarantee_enter`` / ``_guarantee_exit``
helpers, which keep a thread-local stack of "active scopes" and
register every newly-submitted future with whichever scope is on
top.

Module-level singletons :data:`guarantee` and :data:`guarantee_async`
are the user-facing handles -- the classes themselves are private
because there's no point in instantiating more than one.
"""

from __future__ import annotations

import asyncio
from contextlib import suppress

import laila


class _Guarantee:
    """Synchronous context manager that waits for futures created in its scope.

    Pushes a new guarantee scope on the active policy's command stack
    on entry; on exit, blocks until every future registered while the
    scope was active has terminated. If any of those futures errored,
    the first observed exception is re-raised after the wait
    completes -- but only if the body of the ``with`` block did not
    itself raise (in which case the original exception propagates).

    Use as ``with laila.guarantee: ...``. Multiple nested guarantee
    blocks compose: each scope only waits for the futures created
    *between its own __enter__ and __exit__*.
    """

    def __enter__(self):
        """Push a fresh frame onto the active policy's guarantee stack.

        Newly-submitted futures will register with this frame until
        :meth:`__exit__` pops it.
        """
        laila.get_active_policy().central.command._guarantee_enter()
        return self

    def __exit__(self, exc_type, exc, tb):
        """Pop the frame and synchronously wait for every registered future.

        Returns ``False`` so the caller's exception (if any)
        continues to propagate. When the body did not raise but one
        of the in-scope futures did, the first wait-error is raised
        instead.
        """
        created_inside = laila.get_active_policy().central.command._guarantee_exit()

        wait_errors = []
        for future in created_inside:
            try:
                future.wait(timeout=None)
            except Exception as wait_exc:
                wait_errors.append(wait_exc)

        if exc_type is not None:
            return False

        if wait_errors:
            raise wait_errors[0]

        return False


class _AsyncGuaranteeScope:
    """Per-entry state for one open ``async with guarantee_async`` frame.

    A fresh instance is created on every :meth:`_AsyncGuarantee.__aenter__`
    and discarded on the matching :meth:`_AsyncGuarantee.__aexit__`. Keeping
    state here (rather than on the singleton ``_AsyncGuarantee``) is what
    makes :data:`guarantee_async` safely reentrant: nested or concurrent
    ``async with`` blocks each get their own watcher task and their own
    background-exception slot, so the inner frame can no longer clobber
    the outer frame's watcher reference.
    """

    __slots__ = (
        "background_exception",
        "command",
        "parent_task",
        "scope",
        "watcher_task",
    )

    def __init__(self) -> None:
        self.command = None
        self.scope = None
        self.parent_task = None
        self.watcher_task = None
        self.background_exception = None


class _AsyncGuarantee:
    """Asynchronous context manager that awaits futures created in its scope.

    Same lexical guarantee as :class:`_Guarantee`, but for
    coroutine code:

    - Inside the ``async with`` block, a *background watcher* task
      polls the in-scope future list and immediately cancels the
      parent task on the first observed exception. This keeps a
      hung coroutine from sitting forever on a dead future.
    - On exit, every still-pending in-scope future is awaited
      (proper ``await`` for awaitable futures, ``asyncio.to_thread``
      for sync-only ones), then -- if no original exception -- the
      first wait-error or background-error is re-raised.

    Use as ``async with laila.guarantee_async: ...``. Like
    :class:`_Guarantee`, multiple nested scopes compose cleanly: each
    entry allocates its own :class:`_AsyncGuaranteeScope`, kept on a
    per-task stack so concurrent ``async with`` blocks from different
    asyncio tasks never share state.
    """

    def __init__(self) -> None:
        """Initialise the per-task stack used to make the singleton reentrant.

        All real per-scope state lives on :class:`_AsyncGuaranteeScope`
        instances; this dict only tracks which scope belongs to which
        asyncio task so :meth:`__aexit__` can pop the right one.
        """
        self._task_stacks: dict[asyncio.Task, list[_AsyncGuaranteeScope]] = {}

    async def __aenter__(self):
        """Enter the async guarantee scope and start the background watcher."""
        state = _AsyncGuaranteeScope()
        state.command = laila.get_active_policy().central.command
        state.command._guarantee_enter()
        state.scope = state.command._guarantee_stack()[-1]
        state.parent_task = asyncio.current_task()
        state.watcher_task = asyncio.create_task(self._watch_for_future_errors(state))

        self._task_stacks.setdefault(state.parent_task, []).append(state)
        return self

    async def __aexit__(self, exc_type, exc, tb):
        """Exit the scope, cancel the watcher, and await all enclosed futures."""
        task = asyncio.current_task()
        stack = self._task_stacks.get(task)
        if not stack:
            return False
        state = stack.pop()
        if not stack:
            self._task_stacks.pop(task, None)

        created_inside = state.command._guarantee_exit()

        if state.watcher_task is not None and not state.watcher_task.done():
            state.watcher_task.cancel()
            with suppress(asyncio.CancelledError):
                await state.watcher_task

        # If the watcher saw an in-scope future error it cancelled our
        # parent task (see `_watch_for_future_errors`). On Python 3.11+
        # the task stays in cancelled state even after we suppress the
        # first CancelledError, so the next `await` below would re-raise
        # it -- and CancelledError (BaseException) would skip past the
        # caller's `except Exception` handlers and replace the real error.
        # Clear that pending cancellation so the original exception
        # surfaces from `background_exception` instead.
        if state.background_exception is not None:
            current = asyncio.current_task()
            if current is not None and hasattr(current, "uncancel"):
                current.uncancel()

        wait_errors = []
        for future in created_inside:
            try:
                if hasattr(future, "__await__"):
                    await future
                else:
                    await asyncio.to_thread(future.wait, None)
            except Exception as wait_exc:
                wait_errors.append(wait_exc)

        if state.background_exception is not None:
            raise state.background_exception

        if exc_type is not None:
            return False

        if wait_errors:
            raise wait_errors[0]

        return False

    async def _watch_for_future_errors(self, state: _AsyncGuaranteeScope) -> None:
        """Poll for newly registered futures and cancel the parent on error."""
        watched: dict[str, asyncio.Task] = {}

        while True:
            for future_id, future in list(state.scope.items()):
                if future_id in watched:
                    continue
                watched[future_id] = asyncio.create_task(self._await_future(future))

            if not watched:
                await asyncio.sleep(0.01)
                continue

            done, _ = await asyncio.wait(
                watched.values(),
                timeout=0.01,
                return_when=asyncio.FIRST_COMPLETED,
            )

            for future_id, task in list(watched.items()):
                if task not in done:
                    continue
                watched.pop(future_id, None)
                try:
                    await task
                except Exception as exc:
                    state.background_exception = exc
                    if state.parent_task is not None and not state.parent_task.done():
                        state.parent_task.cancel()
                    return

    async def _await_future(self, future):
        """Await a single future, adapting sync futures via ``asyncio.to_thread``."""
        if hasattr(future, "__await__"):
            return await future
        return await asyncio.to_thread(future.wait, None)


guarantee = _Guarantee()
guarantee_async = _AsyncGuarantee()
