"""Slot parking -- release a taskforce slot while awaiting another future.

Problem
-------
A taskforce has a bounded number of execution *slots* (for the default
:class:`PythonAsyncThreadPoolTaskForce` that is ``num_workers *
max_async_per_thread``). A task that holds a slot and then blocks on a
*child* future that itself needs a slot on the same taskforce creates a
hold-and-wait dependency. When every slot is held by such a parent, no
child can ever be dispatched: a classic deadlock. Nesting is pervasive
in laila -- ``laila.remember`` submits per-entry fetches, constitution
bodies call ``manifest.realized`` which remembers children, and users
legitimately ``await laila.remember(...)`` from inside their own
submitted coroutines -- so this must be solved once, in the scheduler,
not by asking every caller to reason about slot budgets.

Solution
--------
Every runner coroutine publishes a :class:`_SlotCtx` in the
:data:`_CURRENT_SLOT` context variable. Whenever code that is running
*inside* a slot waits on a laila future (``await fut``, ``fut.wait()``,
``fut.result``/``fut.data``), the wait is wrapped in :func:`park_async`
or :func:`park_sync`. Those helpers

1. hand the slot back to the loop thread (``_park_enter``), so the
   dispatcher -- or a previously parked sibling -- can use it, and
2. re-acquire a slot before returning to the caller (``_park_exit_*``),
   so the invariant "at most ``cap`` coroutines *run* per loop thread"
   still holds for the CPU/IO work between waits.

While parked, the coroutine is suspended at an ``await`` and consumes no
slot; only the parent's stack frame (memory) is retained. Because a
parked task can never hold a slot while it waits for a child, the
wait-for graph has no slot cycle and progress is guaranteed as long as
the leaf tasks terminate.

Re-acquisition is *prioritised*: a loop thread hands a freed slot to a
waiting parked task before decrementing its in-flight count, so parked
parents resume ahead of newly dispatched roots. This bounds the number of
simultaneously live trees instead of letting the dispatcher keep opening
new ones.

Nesting and threads
-------------------
Parking is reentrant per slot. A depth counter in :class:`_SlotCtx`
ensures that a wait nested inside another wait (e.g. ``GroupFuture.wait``
calling each child's ``wait``) releases the slot exactly once and
re-acquires it exactly once, at the outermost wait. The context is
propagated through :func:`asyncio.to_thread` / ``run_in_executor``
(``contextvars.copy_context``), so a *synchronous* constitution body
running on an executor thread that calls ``fut.wait()`` parks the slot of
the coroutine that offloaded it.

Compute permits
---------------
Sync bodies are offloaded to an executor thread. Threads are cheap to
park on but expensive to run concurrently (GIL contention), so the
taskforce bounds the number of *executing* sync bodies with a semaphore
of ``sync_workers`` permits rather than bounding the number of threads.
:data:`_CURRENT_PERMIT` carries the permit held by the current executor
thread; :func:`park_sync` releases it while the body blocks on a laila
future and re-acquires it afterwards. A body that is blocked therefore
consumes neither a slot nor a permit, which is what makes arbitrarily
deep sync nesting safe.

Cycle guard
-----------
:data:`_RESOLVE_CHAIN` carries the tuple of entry global-ids that are
currently being resolved (remembered or built) along the *causal* chain
of nested submissions. It is snapshotted at submit time and re-installed
by the runner, so a child root task inherits its parent's chain even
though it runs on a different loop thread. ``laila.remember`` and
``laila.build`` consult it to raise :class:`CyclicDependencyError`
instead of recursing forever when a constitution ends up depending on
itself.
"""

from __future__ import annotations

import asyncio
import threading
from collections.abc import Awaitable, Callable
from contextvars import ContextVar
from typing import Any, TypeVar

__all__ = [
    "_CURRENT_PERMIT",
    "_CURRENT_SLOT",
    "_RESOLVE_CHAIN",
    "CyclicDependencyError",
    "_Permit",
    "_SlotCtx",
    "check_resolve_cycle",
    "park_async",
    "park_sync",
    "resolve_chain_with",
]

_T = TypeVar("_T")


class CyclicDependencyError(RuntimeError):
    """Raised when an entry resolution depends (transitively) on itself.

    Detected via :data:`_RESOLVE_CHAIN`: if ``laila.remember`` or
    ``laila.build`` is invoked for a global-id that is already on the
    current chain of nested resolutions, the dependency graph has a cycle
    and no amount of scheduling can complete it.
    """


class _SlotCtx:
    """Bookkeeping for one occupied taskforce slot.

    Attributes
    ----------
    tf : object
        The owning taskforce (duck-typed: needs ``_capacity_cv`` and
        ``_stop``).
    lt : object
        The loop thread whose slot this context represents (duck-typed:
        needs ``release_slot``, ``try_reserve_or_wait``, ``cancel_wait``).
    depth : int
        Number of nested parks currently active. Only the transition
        ``0 -> 1`` releases the slot and only ``1 -> 0`` re-acquires it.
    holds_slot : bool
        Whether this context currently owns a slot on ``lt``.
    lock : threading.Lock
        Guards ``depth`` and ``holds_slot`` -- a sync body may park from
        an executor thread while the owning coroutine's loop thread is
        inspecting the same context.
    """

    __slots__ = ("depth", "holds_slot", "lock", "lt", "tf")

    def __init__(self, tf: Any, lt: Any) -> None:
        self.tf = tf
        self.lt = lt
        self.depth = 0
        self.holds_slot = True
        self.lock = threading.Lock()


_CURRENT_SLOT: ContextVar[_SlotCtx | None] = ContextVar("_laila_current_slot", default=None)
"""Slot context of the runner coroutine the current code is executing under.

``None`` when the caller is not inside any taskforce slot (main thread,
plain user threads, the dispatcher). Propagates into executor threads via
``contextvars.copy_context`` so sync bodies can park too.
"""

_RESOLVE_CHAIN: ContextVar[tuple[str, ...]] = ContextVar("_laila_resolve_chain", default=())
"""Tuple of entry global-ids being resolved along the current causal chain."""


class _Permit:
    """A compute permit held by a sync body running on an executor thread.

    ``sem`` is the taskforce-wide semaphore bounding concurrently
    *executing* sync bodies; ``held`` tracks whether this thread currently
    owns one of its permits (released while parked).
    """

    __slots__ = ("held", "sem")

    def __init__(self, sem: threading.Semaphore) -> None:
        self.sem = sem
        self.held = False

    def acquire(self) -> None:
        if not self.held:
            self.sem.acquire()
            self.held = True

    def release(self) -> None:
        if self.held:
            self.held = False
            self.sem.release()


_CURRENT_PERMIT: ContextVar[_Permit | None] = ContextVar("_laila_current_permit", default=None)
"""Compute permit of the executor thread the current sync body runs on."""


# ---------------------------------------------------------------------------
# release / re-acquire primitives
# ---------------------------------------------------------------------------
def _notify_capacity(tf: Any) -> None:
    cv = getattr(tf, "_capacity_cv", None)
    if cv is None:
        return
    with cv:
        cv.notify()


def _park_enter(ctx: _SlotCtx) -> None:
    """Increment park depth; on the outermost park hand the slot back."""
    with ctx.lock:
        ctx.depth += 1
        if ctx.depth > 1 or not ctx.holds_slot:
            return
        ctx.holds_slot = False
    if ctx.lt.release_slot():
        _notify_capacity(ctx.tf)


def _stop_requested(ctx: _SlotCtx) -> bool:
    stop = getattr(ctx.tf, "_stop", None)
    return stop is not None and stop.is_set()


async def _park_exit_async(ctx: _SlotCtx) -> None:
    """Decrement park depth; on the outermost exit re-acquire a slot.

    If the taskforce is shutting down the re-acquire is skipped: the
    task will be cancelled by the loop teardown anyway and must not block
    on a slot that will never be handed out.
    """
    with ctx.lock:
        ctx.depth -= 1
        if ctx.depth > 0 or ctx.holds_slot:
            return
    if _stop_requested(ctx):
        return
    loop = asyncio.get_running_loop()
    fut: asyncio.Future[None] = loop.create_future()

    def _wake() -> None:
        if not fut.done():
            fut.set_result(None)

    def cb() -> None:
        try:
            loop.call_soon_threadsafe(_wake)
        except RuntimeError:
            # Loop closed during shutdown; nothing to resume.
            pass

    if ctx.lt.try_reserve_or_wait(cb):
        with ctx.lock:
            ctx.holds_slot = True
        return
    try:
        await fut
    except BaseException:
        # Cancelled while waiting. If the slot was already handed to us we
        # own it and the runner's finally-block must release it.
        if not ctx.lt.cancel_wait(cb):
            with ctx.lock:
                ctx.holds_slot = True
        raise
    with ctx.lock:
        ctx.holds_slot = True


def _park_exit_sync(ctx: _SlotCtx) -> None:
    """Sync counterpart of :func:`_park_exit_async` for executor threads."""
    with ctx.lock:
        ctx.depth -= 1
        if ctx.depth > 0 or ctx.holds_slot:
            return
    if _stop_requested(ctx):
        return
    ev = threading.Event()
    cb = ev.set
    if ctx.lt.try_reserve_or_wait(cb):
        with ctx.lock:
            ctx.holds_slot = True
        return
    while not ev.wait(0.05):
        if _stop_requested(ctx):
            if ctx.lt.cancel_wait(cb):
                return
            break
    with ctx.lock:
        ctx.holds_slot = True


# ---------------------------------------------------------------------------
# public helpers
# ---------------------------------------------------------------------------
async def park_async(awaitable: Awaitable[_T]) -> _T:
    """Await *awaitable*, parking the current slot (if any) for the duration.

    Outside a taskforce slot this is a plain ``await``.
    """
    ctx = _CURRENT_SLOT.get()
    if ctx is None:
        return await awaitable
    _park_enter(ctx)
    try:
        return await awaitable
    finally:
        await _park_exit_async(ctx)


def park_sync(fn: Callable[..., _T], *args: Any, **kwargs: Any) -> _T:
    """Call ``fn(*args, **kwargs)``, parking the current slot (if any).

    Intended for blocking waits executed on executor threads that were
    spawned from inside a slot (``asyncio.to_thread`` copies the context,
    so :data:`_CURRENT_SLOT` is visible there). Outside a slot this is a
    plain call.
    """
    ctx = _CURRENT_SLOT.get()
    permit = _CURRENT_PERMIT.get()
    if ctx is None and permit is None:
        return fn(*args, **kwargs)
    # Give up the compute permit first (never hold a permit while waiting),
    # then the slot.
    released_permit = permit is not None and permit.held
    if released_permit:
        permit.release()
    if ctx is not None:
        _park_enter(ctx)
    try:
        return fn(*args, **kwargs)
    finally:
        # Re-acquire in the opposite order: slot, then permit. A thread that
        # holds a slot and waits for a permit cannot deadlock because every
        # permit holder is executing (and will finish or park).
        if ctx is not None:
            _park_exit_sync(ctx)
        if released_permit:
            permit.acquire()


def check_resolve_cycle(*global_ids: str) -> None:
    """Raise :class:`CyclicDependencyError` if any id is already being resolved."""
    chain = _RESOLVE_CHAIN.get()
    if not chain:
        return
    for gid in global_ids:
        if gid in chain:
            raise CyclicDependencyError(
                f"Cyclic dependency: {gid} is already being resolved on the "
                f"current chain ({' -> '.join(chain)} -> {gid})."
            )


def resolve_chain_with(*global_ids: str) -> tuple[str, ...]:
    """Return the current chain extended with *global_ids* (no mutation)."""
    return _RESOLVE_CHAIN.get() + tuple(global_ids)
