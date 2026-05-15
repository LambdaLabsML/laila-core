"""Async-thread-pool taskforce — N owned threads, each running its own asyncio loop.

Each loop hosts up to ``max_async_per_thread`` concurrent in-flight tasks.
Submitted callables must be coroutine functions (zero-arg). Plain sync
callables are auto-wrapped at the ``Command.submit`` boundary into trivial
``async def`` shims so the runner here only ever sees coroutine functions.
A wrapped sync body still runs inline on its loop thread for the duration
of the call — it blocks other coroutines on the same loop. Async bodies
yield on every ``await`` and let the loop interleave other in-flight
coroutines.

The dispatcher routes each task to the loop with the lowest current
in-flight count whose count is strictly below ``max_async_per_thread``;
when every loop is at the cap, the dispatcher blocks until one frees up.

Each :class:`_LoopThread` registers its OS thread id with
``_ASYNC_LOOP_THREAD_IDS`` while running so the future ``wait()`` guard
can detect (and reject) blocking waits called from inside a loop thread.

Reuses :class:`ConcurrentPackageFuture` for the per-task future state;
the runner coroutine sets ``fut.exception``, ``fut.result``, and
``fut.status`` directly.
"""
from __future__ import annotations
import asyncio
import inspect
import os
import threading
from typing import Callable, Any, Iterable, List, Union, Optional

from pydantic import Field, PrivateAttr, ConfigDict

from ..thread_pool_executor.future import ConcurrentPackageFuture
from ...schema.future.future.group_future import GroupFuture
from ...schema.future.future.future_status import FutureStatus
from ...schema.exceptions import (
    _register_async_loop_thread,
    _unregister_async_loop_thread,
)
from ..status import TaskForceStatus
from ..base import _LAILA_IDENTIFIABLE_TASK_FORCE


class _LoopThread:
    """A single owned thread running its own asyncio event loop.

    Concurrency budget per loop is tracked via ``inflight``; the dispatcher
    consults :meth:`inflight_count` to make routing decisions and calls
    :meth:`reserve_slot` / :meth:`release_slot` around each scheduled task.

    Registers its thread id with the global ``_ASYNC_LOOP_THREAD_IDS``
    set while running so blocking ``Future.wait()`` calls from inside the
    loop can be rejected with :exc:`LoopBlockingWaitError`.
    """

    def __init__(self, name: str) -> None:
        self.name = name
        self.loop: asyncio.AbstractEventLoop = asyncio.new_event_loop()
        self._inflight: int = 0
        self._inflight_lock = threading.Lock()
        self._ready = threading.Event()
        self._thread_ident: Optional[int] = None
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
                self.loop.run_until_complete(asyncio.sleep(0))
            except Exception:
                pass
            self.loop.close()
            if self._thread_ident is not None:
                _unregister_async_loop_thread(self._thread_ident)

    def inflight_count(self) -> int:
        with self._inflight_lock:
            return self._inflight

    def reserve_slot(self) -> None:
        with self._inflight_lock:
            self._inflight += 1

    def release_slot(self) -> None:
        with self._inflight_lock:
            if self._inflight > 0:
                self._inflight -= 1

    def submit_coro(self, coro):
        return asyncio.run_coroutine_threadsafe(coro, self.loop)

    def stop(self, timeout: Optional[float] = None) -> None:
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
      loop. Sync tasks consume a slot for their full inline duration;
      async tasks consume a slot from scheduling until the coroutine
      returns/raises.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    backend: str = Field(default="async_threads", description="Execution backend (async_threads only).")
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

    _cv: Optional[threading.Condition] = PrivateAttr(default=None)
    _capacity_cv: Optional[threading.Condition] = PrivateAttr(default=None)
    _stop: Optional[threading.Event] = PrivateAttr(default=None)
    _dispatcher: Optional[threading.Thread] = PrivateAttr(default=None)
    _loops: List[_LoopThread] = PrivateAttr(default_factory=list)

    def _on_start(self) -> None:
        if self.backend.lower() != "async_threads":
            raise ValueError("PythonAsyncThreadPoolTaskForce supports async_threads only.")

        self._cv = threading.Condition()
        self._capacity_cv = threading.Condition()
        self._stop = threading.Event()
        self._loops = [
            _LoopThread(name=f"AsyncTF-{self.global_id[-8:]}-Loop-{i}")
            for i in range(self.num_workers)
        ]
        self._dispatcher = threading.Thread(
            target=self._loop, name=f"AsyncTF-{self.global_id[-8:]}-Dispatcher", daemon=True
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
                self._q[fut.global_id] = (None, args, kwargs)
            self._cv.notify()

        return fut

    def imap(self, tasks: Iterable[Callable[[], Any]]) -> Iterable[Any]:
        for f in tasks:
            fut = self._queue_submit(f)
            yield fut.future_identity

    def submit(
        self,
        tasks: Iterable[Callable[[], Any]],
        wait: bool = False,
    ) -> Union[GroupFuture, Any]:
        tasks = list(tasks)

        futures: List[ConcurrentPackageFuture] = []
        for task in tasks:
            fut = self._queue_submit(task)
            fut.taskforce_id = self.global_id
            futures.append(fut)

        if len(futures) == 1:
            single = futures[0]
            if wait:
                return single.wait(None)
            return single.future_identity

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

    def _pick_loop(self) -> Optional[_LoopThread]:
        """Return the loop with the lowest in-flight count under the cap, or None."""
        cap = self.max_async_per_thread
        best: Optional[_LoopThread] = None
        best_count = cap
        for lt in self._loops:
            c = lt.inflight_count()
            if c < best_count:
                best_count = c
                best = lt
        return best

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

            picked: Optional[_LoopThread] = None
            while not stop.is_set() and picked is None:
                picked = self._pick_loop()
                if picked is not None:
                    picked.reserve_slot()
                    break
                with cap_cv:
                    cap_cv.wait(timeout=0.1)

            if stop.is_set():
                with self._q.atomic():
                    self._q[kwargs["fut"].global_id] = (None, args, kwargs)
                break

            task = kwargs["task"]
            fut = kwargs["fut"]
            user_kwargs = {k: v for k, v in kwargs.items() if k not in {"task", "fut"}}

            try:
                coro = self._make_runner_coro(task, args, user_kwargs, fut, picked)
                picked.submit_coro(coro)
            except Exception as exc:
                picked.release_slot()
                with cap_cv:
                    cap_cv.notify()
                fut.exception = exc
                fut.result = None
                fut.status = FutureStatus.ERROR

    def _make_runner_coro(self, task, args, kwargs, fut, lt: _LoopThread):
        cap_cv = self._capacity_cv

        async def _runner():
            fut.status = FutureStatus.RUNNING
            try:
                out = task(*args, **kwargs)
                if inspect.iscoroutine(out):
                    out = await out
                fut.exception = None
                fut.result = out
                fut.status = FutureStatus.FINISHED
            except Exception as exc:
                fut.exception = exc
                fut.result = None
                fut.status = FutureStatus.ERROR
            finally:
                lt.release_slot()
                with cap_cv:
                    cap_cv.notify()

        return _runner()
