"""Async-thread-pool taskforce — N owned threads, each running its own asyncio loop.

Each loop hosts up to ``max_async_per_thread`` concurrent in-flight tasks
(*slots*). Submitted callables must be coroutine functions (zero-arg).
Plain sync callables are auto-wrapped at the ``Command.submit`` boundary
into ``async def`` shims that offload the body to this taskforce's
``sync`` executor (see ``ensure_coroutine_function``), so a sync body
never blocks a loop thread. Async bodies yield on every ``await`` and let
the loop interleave other in-flight coroutines.

The dispatcher routes each task to the loop with the lowest current
in-flight count whose count is strictly below ``max_async_per_thread``;
when every loop is at the cap, the dispatcher blocks until one frees up.

Slot parking
------------
A task that awaits a laila future while holding a slot *parks*: the slot
is released for the duration of the wait and re-acquired afterwards (see
:mod:`laila.policy.central.command.schema.parking`). Parked tasks are
resumed with priority over freshly dispatched roots -- a loop thread
hands a freed slot to a waiting parked task before it decrements its
in-flight count. This makes nested submissions (``await laila.remember``
inside a submitted coroutine, constitution bodies realising manifests,
...) deadlock-free regardless of ``num_workers`` and
``max_async_per_thread``.

Sync offload
------------
Sync bodies (auto-wrapped sync callables and constitution bodies) run via
:meth:`run_sync` on an owned executor whose *thread count* is effectively
unbounded, while the number of bodies *executing* at any time is bounded
by ``sync_workers`` compute permits (a semaphore). A body that blocks on
a laila future releases both its slot and its permit
(:func:`~laila.policy.central.command.schema.parking.park_sync`), so
deep nesting of blocking sync bodies can never exhaust the pool -- a
bounded thread pool would reintroduce the hold-and-wait pattern one level
down -- while CPU parallelism stays capped at a sane level. The executor
is also installed as each loop's default executor so ``asyncio.to_thread``
inside user coroutines lands on it (without a permit).

Each :class:`_LoopThread` registers its OS thread id with
``_ASYNC_LOOP_THREAD_IDS`` while running so the future ``wait()`` guard
can detect (and reject) blocking waits called from inside a loop thread.

Reuses :class:`ConcurrentPackageFuture` for the per-task future state;
the runner coroutine sets ``fut.exception``, ``fut.result``, and
``fut.status`` directly.
"""

from __future__ import annotations

import asyncio
import contextvars
import functools
import inspect
import os
import threading
from collections import deque
from collections.abc import Callable, Iterable
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from pydantic import ConfigDict, Field, PrivateAttr

from ...schema.exceptions import (
    _register_async_loop_thread,
    _unregister_async_loop_thread,
)
from ...schema.future.future.future_status import FutureStatus
from ...schema.future.future.group_future import GroupFuture
from ...schema.parking import (
    _CURRENT_PERMIT,
    _CURRENT_SLOT,
    _RESOLVE_CHAIN,
    _Permit,
    _SlotCtx,
)
from ..base import _LAILA_IDENTIFIABLE_TASK_FORCE
from ..status import TaskForceStatus
from ..thread_pool_executor.future import ConcurrentPackageFuture

_CHAIN_KW = "_laila_resolve_chain"
_EXECUTOR_MAX_WORKERS = 1_000_000


class _LoopThread:
    """A single owned thread running its own asyncio event loop.

    Concurrency budget per loop is tracked via ``_inflight`` against
    ``cap``; the dispatcher calls :meth:`try_reserve` to claim a slot for
    a new root task, runners and parked tasks call :meth:`release_slot`
    and :meth:`try_reserve_or_wait`.

    Slot hand-off
    -------------
    ``_resume_waiters`` holds callbacks of parked tasks waiting to get a
    slot back. :meth:`release_slot` pops the oldest waiter *instead of*
    decrementing the in-flight count, so the slot is transferred directly
    to that parked task and the dispatcher never sees it as free. Only
    when nobody is waiting is the count decremented and ``True`` returned
    so the caller can notify the dispatcher's capacity condition.

    Registers its thread id with the global ``_ASYNC_LOOP_THREAD_IDS``
    set while running so blocking ``Future.wait()`` calls from inside the
    loop can be rejected with :exc:`LoopBlockingWaitError`.
    """

    def __init__(self, name: str, cap: int) -> None:
        self.name = name
        self.cap = cap
        self.loop: asyncio.AbstractEventLoop = asyncio.new_event_loop()
        self._inflight: int = 0
        self._inflight_lock = threading.Lock()
        self._resume_waiters: deque[Callable[[], None]] = deque()
        self._ready = threading.Event()
        self._thread_ident: int | None = None
        self.thread = threading.Thread(target=self._run, name=name, daemon=True)
        self.thread.start()
        self._ready.wait()

    def _run(self) -> None:
        asyncio.set_event_loop(self.loop)
        self._thread_ident = threading.get_ident()
        _register_async_loop_thread(self._thread_ident)
        self.loop.call_soon(self._ready.set)
        try:
            self.loop.run_forever()
        finally:
            try:
                pending = asyncio.all_tasks(loop=self.loop)
                for t in pending:
                    t.cancel()
                # A few iterations so cancelled tasks can unwind their
                # ``finally`` blocks (parking exit, runner bookkeeping).
                for _ in range(3):
                    if not asyncio.all_tasks(loop=self.loop):
                        break
                    self.loop.run_until_complete(asyncio.sleep(0))
            except Exception:
                pass
            self.loop.close()
            if self._thread_ident is not None:
                _unregister_async_loop_thread(self._thread_ident)

    # ---------- slot accounting ----------
    def inflight_count(self) -> int:
        with self._inflight_lock:
            return self._inflight

    def parked_waiting(self) -> int:
        """Number of parked tasks currently waiting to re-acquire a slot."""
        with self._inflight_lock:
            return len(self._resume_waiters)

    def try_reserve(self) -> bool:
        """Claim a slot for a new root task if under the cap."""
        with self._inflight_lock:
            if self._inflight >= self.cap:
                return False
            self._inflight += 1
            return True

    def try_reserve_or_wait(self, cb: Callable[[], None]) -> bool:
        """Claim a slot now (``True``) or enqueue *cb* to be handed one later.

        Used by parked tasks re-acquiring. The callback is invoked -- from
        whichever thread releases the slot -- exactly once when a slot has
        been transferred to the waiter.
        """
        with self._inflight_lock:
            if self._inflight < self.cap:
                self._inflight += 1
                return True
            self._resume_waiters.append(cb)
            return False

    def cancel_wait(self, cb: Callable[[], None]) -> bool:
        """Remove *cb* from the waiters. ``False`` if it was already served."""
        with self._inflight_lock:
            try:
                self._resume_waiters.remove(cb)
                return True
            except ValueError:
                return False

    def release_slot(self) -> bool:
        """Give up one slot.

        Returns ``True`` if the in-flight count was decremented (the
        dispatcher may now place a new root), ``False`` if the slot was
        transferred to a parked waiter instead.
        """
        with self._inflight_lock:
            if self._resume_waiters:
                cb = self._resume_waiters.popleft()
            else:
                cb = None
                if self._inflight > 0:
                    self._inflight -= 1
        if cb is not None:
            cb()
            return False
        return True

    def submit_coro(self, coro):
        return asyncio.run_coroutine_threadsafe(coro, self.loop)

    def stop(self, timeout: float | None = None) -> None:
        try:
            self.loop.call_soon_threadsafe(self.loop.stop)
        except RuntimeError:
            pass
        self.thread.join(timeout=timeout)


class PythonAsyncThreadPoolTaskForce(_LAILA_IDENTIFIABLE_TASK_FORCE):
    """TaskForce that owns N threads, each running its own asyncio loop.

    Concurrency knobs:

    - ``num_workers`` — number of owned threads (each with its own loop).
    - ``max_async_per_thread`` — maximum concurrent in-flight tasks per
      loop. A task consumes a slot from scheduling until the coroutine
      returns/raises, *minus* the time it spends parked on laila futures.
    - ``sync_workers`` — maximum number of sync bodies *executing*
      concurrently on executor threads (bodies blocked on laila futures
      do not count).
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    backend: str = Field(
        default="async_threads", description="Execution backend (async_threads only)."
    )
    num_workers: int = Field(
        default_factory=lambda: max(1, os.cpu_count() or 1),
        ge=1,
        description="Number of worker threads, each running its own asyncio event loop.",
    )
    max_async_per_thread: int = Field(
        default=64,
        ge=1,
        description="Maximum concurrent in-flight tasks per loop thread.",
    )
    sync_workers: int = Field(
        default_factory=lambda: max(4, (os.cpu_count() or 1) * 2),
        ge=1,
        description=(
            "Maximum number of plain (non-async) bodies executing concurrently "
            "on executor threads; bodies parked on laila futures do not count."
        ),
    )

    _cv: threading.Condition | None = PrivateAttr(default=None)
    _capacity_cv: threading.Condition | None = PrivateAttr(default=None)
    _stop: threading.Event | None = PrivateAttr(default=None)
    _dispatcher: threading.Thread | None = PrivateAttr(default=None)
    _loops: list[_LoopThread] = PrivateAttr(default_factory=list)
    _executor: ThreadPoolExecutor | None = PrivateAttr(default=None)
    _compute_permits: threading.Semaphore | None = PrivateAttr(default=None)

    def _on_start(self) -> None:
        if self.backend.lower() != "async_threads":
            raise ValueError("PythonAsyncThreadPoolTaskForce supports async_threads only.")

        tag = self.global_id[-8:]
        self._cv = threading.Condition()
        self._capacity_cv = threading.Condition()
        self._stop = threading.Event()
        self._executor = ThreadPoolExecutor(
            max_workers=_EXECUTOR_MAX_WORKERS, thread_name_prefix=f"AsyncTF-{tag}-Sync"
        )
        self._compute_permits = threading.Semaphore(self.sync_workers)
        self._loops = [
            _LoopThread(name=f"AsyncTF-{tag}-Loop-{i}", cap=self.max_async_per_thread)
            for i in range(self.num_workers)
        ]
        for lt in self._loops:
            lt.loop.set_default_executor(self._executor)
        self._dispatcher = threading.Thread(
            target=self._loop, name=f"AsyncTF-{tag}-Dispatcher", daemon=True
        )
        self._dispatcher.start()

    def _on_pause(self) -> None:
        raise NotImplementedError

    def _on_shutdown(self, *, wait: bool = True, cancel_pending: bool = True) -> None:
        if self._stop is not None:
            self._stop.set()
        if self._cv is not None:
            with self._cv:
                self._cv.notify_all()
        if self._capacity_cv is not None:
            with self._capacity_cv:
                self._capacity_cv.notify_all()

        if wait and self._dispatcher is not None:
            self._dispatcher.join()

        if cancel_pending:
            with self._q.atomic("cancel"):
                for _, (_, _, kwargs) in self._q.items():
                    fut = kwargs.get("fut")
                    if fut is None:
                        continue
                    fut.exception = RuntimeError("Task canceled before dispatch.")
                    fut.status = FutureStatus.CANCELLED
                    fut.result = None
                self._q.clear()

        for lt in self._loops:
            lt.stop(timeout=None if wait else 0.0)

        if self._executor is not None:
            self._executor.shutdown(wait=wait, cancel_futures=cancel_pending)

    # =========================================================
    # Sync offload
    # =========================================================

    def run_sync(self, fn: Callable[..., Any], *args: Any, **kwargs: Any) -> asyncio.Future:
        """Run ``fn(*args, **kwargs)`` on an executor thread under a compute permit.

        Must be called from a coroutine running on one of this taskforce's
        loops. The current context (slot, resolve chain) is propagated so
        the body can park its slot -- and release its permit -- when it
        blocks on laila futures. Returns an awaitable ``asyncio.Future``.
        """
        executor = self._executor
        permits = self._compute_permits
        if executor is None or permits is None:
            raise RuntimeError("TaskForce must be running before offloading sync work.")
        loop = asyncio.get_running_loop()
        ctx = contextvars.copy_context()

        def _body():
            permit = _Permit(permits)
            _CURRENT_PERMIT.set(permit)
            permit.acquire()
            try:
                return fn(*args, **kwargs)
            finally:
                permit.release()

        return loop.run_in_executor(executor, functools.partial(ctx.run, _body))

    # =========================================================
    # Observability
    # =========================================================

    @property
    def inflight(self) -> int:
        """Slots currently occupied across all loops."""
        return sum(lt.inflight_count() for lt in self._loops)

    @property
    def parked(self) -> int:
        """Parked tasks currently waiting to re-acquire a slot."""
        return sum(lt.parked_waiting() for lt in self._loops)

    # =========================================================
    # Submission
    # =========================================================

    def _queue_submit(self, task: Callable[..., Any], *args, **kwargs) -> ConcurrentPackageFuture:
        if self.status != TaskForceStatus.RUNNING:
            raise RuntimeError("TaskForce must be running before submitting tasks.")

        fut = ConcurrentPackageFuture(
            taskforce_id=self.global_id,
            policy_id=self.policy_id,
        )

        with self._cv:
            with self._q.atomic():
                kwargs["task"] = task
                kwargs["fut"] = fut
                # Snapshot the caller's resolve chain so the child root
                # inherits it even though it runs on another loop thread.
                kwargs[_CHAIN_KW] = _RESOLVE_CHAIN.get()
                self._q[fut.global_id] = (None, args, kwargs)
            self._cv.notify()

        return fut

    def imap(self, tasks: Iterable[Callable[[], Any]]) -> Iterable[Any]:
        # Yield the future itself: it already *is* a _LAILA_IDENTIFIABLE_FUTURE,
        # so building a separate identity handle per task is pure overhead.
        for f in tasks:
            yield self._queue_submit(f)

    def submit(
        self,
        tasks: Iterable[Callable[[], Any]],
        wait: bool = False,
    ) -> GroupFuture | Any:
        tasks = list(tasks)

        futures: list[ConcurrentPackageFuture] = []
        for task in tasks:
            fut = self._queue_submit(task)
            fut.taskforce_id = self.global_id
            futures.append(fut)

        if len(futures) == 1:
            single = futures[0]
            if wait:
                return single.wait(None)
            # Return the concrete future (an identity-compatible object)
            # rather than a fresh identity handle; accessors resolve
            # directly instead of scanning every local policy's bank.
            return single

        gf = GroupFuture(
            taskforce_id=self.global_id,
            policy_id=self.policy_id,
            future_ids=[f.global_id for f in futures],
        )

        for f in futures:
            f.future_group_id = gf.global_id

        if not wait:
            return gf
        return gf.wait(None)

    # =========================================================
    # Dispatcher
    # =========================================================

    def _pick_loop(self) -> _LoopThread | None:
        """Reserve a slot on the loop with the lowest in-flight count, or None."""
        cap = self.max_async_per_thread
        best: _LoopThread | None = None
        best_count = cap
        for lt in self._loops:
            c = lt.inflight_count()
            if c < best_count:
                best_count = c
                best = lt
        if best is not None and best.try_reserve():
            return best
        return None

    def _loop(self) -> None:
        cv = self._cv
        cap_cv = self._capacity_cv
        stop = self._stop

        while not stop.is_set():
            with cv:
                while not stop.is_set() and len(self._q) == 0:
                    cv.wait(timeout=0.1)
                if stop.is_set():
                    break
                _, item = self._q.pop_next()
                _, args, kwargs = item

            picked: _LoopThread | None = None
            while not stop.is_set() and picked is None:
                picked = self._pick_loop()
                if picked is not None:
                    break
                with cap_cv:
                    cap_cv.wait(timeout=0.1)

            if stop.is_set():
                with self._q.atomic():
                    self._q[kwargs["fut"].global_id] = (None, args, kwargs)
                break

            task = kwargs["task"]
            fut = kwargs["fut"]
            chain = kwargs.get(_CHAIN_KW, ())
            user_kwargs = {k: v for k, v in kwargs.items() if k not in {"task", "fut", _CHAIN_KW}}

            try:
                coro = self._make_runner_coro(task, args, user_kwargs, fut, picked, chain)
                picked.submit_coro(coro)
            except Exception as exc:
                if picked.release_slot():
                    with cap_cv:
                        cap_cv.notify()
                fut.exception = exc
                fut.result = None
                fut.status = FutureStatus.ERROR

    def _make_runner_coro(self, task, args, kwargs, fut, lt: _LoopThread, chain=()):
        cap_cv = self._capacity_cv

        async def _runner():
            ctx = _SlotCtx(self, lt)
            slot_token = _CURRENT_SLOT.set(ctx)
            chain_token = _RESOLVE_CHAIN.set(tuple(chain))
            fut.status = FutureStatus.RUNNING
            try:
                out = task(*args, **kwargs)
                if inspect.iscoroutine(out):
                    out = await out
                fut.exception = None
                fut.result = out
                fut.status = FutureStatus.FINISHED
            except asyncio.CancelledError:
                fut.exception = RuntimeError("Task cancelled during taskforce shutdown.")
                fut.result = None
                fut.status = FutureStatus.CANCELLED
                raise
            except Exception as exc:
                fut.exception = exc
                fut.result = None
                fut.status = FutureStatus.ERROR
            finally:
                _RESOLVE_CHAIN.reset(chain_token)
                _CURRENT_SLOT.reset(slot_token)
                with ctx.lock:
                    holds = ctx.holds_slot
                    ctx.holds_slot = False
                if holds and lt.release_slot():
                    with cap_cv:
                        cap_cv.notify()

        return _runner()
