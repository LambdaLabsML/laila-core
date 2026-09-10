"""Exceptions and runtime guards for the central command system.

Provides:

- :exc:`LoopBlockingWaitError` — raised when ``Future.wait()`` is called
  from a thread that is currently driving an async taskforce event loop
  (which would deadlock the loop on a future the loop itself needs to
  advance).
- :exc:`NestedCommandSubmitError` — raised when a function decorated
  ``@no_command_submit`` synchronously invokes ``Command.submit``.
- :func:`no_command_submit` — decorator that marks a function as forbidden
  from calling ``Command.submit`` while it executes; works on both sync and
  async callables via a shared ``ContextVar``.
- :data:`_ASYNC_LOOP_THREAD_IDS` — module-level set of thread ids that own
  an async event loop. Populated/cleared by :class:`_LoopThread` in the
  async taskforce. Consulted by future ``wait()`` implementations.
- :data:`_NO_SUBMIT_OWNER` — ``ContextVar`` carrying the qualname of the
  innermost ``@no_command_submit`` function currently executing on the
  context. Read by ``Command.submit`` to enforce the contract.
"""

from __future__ import annotations

import asyncio
import functools
import inspect
import threading
from contextvars import ContextVar

_ASYNC_LOOP_THREAD_IDS: set[int] = set()
_ASYNC_LOOP_THREAD_IDS_LOCK = threading.Lock()


def _register_async_loop_thread(thread_ident: int) -> None:
    """Mark *thread_ident* as owning an async event loop."""
    with _ASYNC_LOOP_THREAD_IDS_LOCK:
        _ASYNC_LOOP_THREAD_IDS.add(thread_ident)


def _unregister_async_loop_thread(thread_ident: int) -> None:
    """Drop *thread_ident* from the async-loop registry."""
    with _ASYNC_LOOP_THREAD_IDS_LOCK:
        _ASYNC_LOOP_THREAD_IDS.discard(thread_ident)


class LoopBlockingWaitError(RuntimeError):
    """Raised when ``Future.wait()`` is called from inside an async loop thread.

    The blocking wait would freeze the loop on a future that needs the
    same loop to advance. Use ``await fut`` from inside a coroutine
    instead.
    """


def _check_not_loop_thread() -> None:
    """Raise :exc:`LoopBlockingWaitError` if the current thread owns a loop."""
    if threading.get_ident() in _ASYNC_LOOP_THREAD_IDS:
        raise LoopBlockingWaitError(
            "Future.wait() called from inside an async taskforce loop thread. "
            "Use `await fut` instead of `.wait()` from coroutines."
        )


_NO_SUBMIT_OWNER: ContextVar[str | None] = ContextVar("_no_submit_owner", default=None)


class NestedCommandSubmitError(RuntimeError):
    """Raised when a ``@no_command_submit`` function calls ``cmd.submit``."""


def no_command_submit(fn):
    """Mark *fn* as forbidden from synchronously invoking ``cmd.submit``.

    Sets a ``ContextVar`` while *fn* runs; ``Command.submit`` reads the
    var and raises :exc:`NestedCommandSubmitError` if it is set. Works
    for both sync and async callables. The contextvar propagates through
    ``await`` and across ``asyncio.create_task`` boundaries.
    """
    if inspect.iscoroutinefunction(fn):

        @functools.wraps(fn)
        async def _async_wrap(*args, **kwargs):
            token = _NO_SUBMIT_OWNER.set(fn.__qualname__)
            try:
                return await fn(*args, **kwargs)
            finally:
                _NO_SUBMIT_OWNER.reset(token)

        return _async_wrap

    @functools.wraps(fn)
    def _sync_wrap(*args, **kwargs):
        token = _NO_SUBMIT_OWNER.set(fn.__qualname__)
        try:
            return fn(*args, **kwargs)
        finally:
            _NO_SUBMIT_OWNER.reset(token)

    return _sync_wrap


def _check_no_pending_submit_owner() -> None:
    """Raise :exc:`NestedCommandSubmitError` if ``@no_command_submit`` is active."""
    owner = _NO_SUBMIT_OWNER.get()
    if owner is not None:
        raise NestedCommandSubmitError(
            f"`{owner}` is decorated `@no_command_submit` but called "
            "`Command.submit`. Build functions must not nest command "
            "submissions; use `await` on a future passed in from outside "
            "the build, or restructure to submit from the orchestrator."
        )


def ensure_coroutine_function(task):
    """Return a coroutine function for *task*.

    Coroutine functions (including ``functools.partial`` of one) are
    returned unchanged. Plain sync callables are wrapped in an
    ``async def`` shim so the runner sees a uniform awaitable. The shim
    offloads the sync body to the owning taskforce's sync executor
    (:meth:`PythonAsyncThreadPoolTaskForce.run_sync`) -- or
    :func:`asyncio.to_thread` when not running inside a taskforce slot --
    so a CPU-bound or blocking body never stalls the event loop. If the
    body itself returns a coroutine (e.g. a lambda factory
    ``lambda: my_async_fn()``), the shim awaits it on the loop so the
    runner gets the final value rather than an unawaited coroutine.

    Prefer submitting coroutine functions / ``functools.partial(coro_fn,
    ...)`` for async work: they skip the thread hop entirely.
    """
    if inspect.iscoroutinefunction(task):
        return task

    @functools.wraps(task)
    async def _wrap(*args, **kwargs):
        from .parking import _CURRENT_SLOT

        slot = _CURRENT_SLOT.get()
        tf = getattr(slot, "tf", None)
        if tf is not None and hasattr(tf, "run_sync"):
            out = await tf.run_sync(task, *args, **kwargs)
        else:
            out = await asyncio.to_thread(task, *args, **kwargs)
        if inspect.iscoroutine(out):
            out = await out
        return out

    return _wrap
