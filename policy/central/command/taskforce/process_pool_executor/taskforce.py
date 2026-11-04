"""Process-pool backed taskforce using ``concurrent.futures.ProcessPoolExecutor``."""

from __future__ import annotations

import logging
import multiprocessing
import os
import pickle
import threading
from collections.abc import Callable, Iterable
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures.process import BrokenProcessPool
from typing import Any, ClassVar

from pydantic import ConfigDict, Field, PrivateAttr

from ...schema.future.future.future_status import FutureStatus
from ...schema.future.future.group_future import GroupFuture
from ..base import _LAILA_IDENTIFIABLE_TASK_FORCE
from ..status import TaskForceStatus
from .future import ProcessPackageFuture

_log = logging.getLogger(__name__)


def _process_runner(task: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]) -> Any:
    """Top-level picklable function that executes a task in a worker process."""
    return task(*args, **kwargs)


class PythonProcessPoolTaskForce(_LAILA_IDENTIFIABLE_TASK_FORCE):
    """
    Process-pool TaskForce implementation.

    This backend mirrors the thread-pool task force, but only accepts
    top-level picklable callables and picklable args/kwargs/results.

    Worker death
    ------------
    When a worker process dies (``os._exit``, segfault, OOM kill) the
    stdlib marks the executor *broken*: every in-flight future fails
    with :class:`BrokenProcessPool` and the executor refuses new work.
    The dispatcher recovers by building a fresh executor with the same
    spawn context and re-submitting the task it was about to place;
    queued tasks keep flowing. Only if that rebuild itself fails does the
    taskforce flip to :attr:`TaskForceStatus.CRASHED`, fail the pending
    queue and stop accepting submissions.
    """

    # Callables cross a process boundary: ``Command.submit`` must hand
    # over the original picklable objects, not the coroutine shim.
    _wraps_sync: ClassVar[bool] = False

    model_config = ConfigDict(arbitrary_types_allowed=True)

    backend: str = Field(default="processes", description="Execution backend (processes only).")
    num_workers: int = Field(
        default_factory=lambda: max(4, (os.cpu_count() or 1) // 2),
        ge=4,
        description="Number of worker processes (minimum 4).",
    )

    _cv: threading.Condition | None = PrivateAttr(default=None)
    _worker_pool: ProcessPoolExecutor | None = PrivateAttr(default=None)
    _stop: threading.Event | None = PrivateAttr(default=None)
    _dispatcher: threading.Thread | None = PrivateAttr(default=None)
    _submit_slots: threading.Semaphore | None = PrivateAttr(default=None)
    _mp_context: Any = PrivateAttr(default=None)

    def _on_start(self) -> None:
        """Initialize process pool and dispatcher."""
        if self.backend.lower() != "processes":
            raise ValueError("PythonProcessPoolTaskForce supports processes only.")

        self._cv = threading.Condition()
        self._stop = threading.Event()
        self._mp_context = multiprocessing.get_context("spawn")
        self._worker_pool = self._new_pool()
        self._submit_slots = threading.Semaphore(max(1, self.num_workers * 2))
        self._dispatcher = threading.Thread(
            target=self._loop, name="ProcessTaskForce-Dispatcher", daemon=True
        )
        self._dispatcher.start()

    def _new_pool(self) -> ProcessPoolExecutor:
        return ProcessPoolExecutor(
            max_workers=self.num_workers,
            mp_context=self._mp_context,
        )

    def _on_pause(self) -> None:
        """Pause dispatcher loop without destroying the pool (currently a no-op)."""
        raise NotImplementedError

    def _on_shutdown(self, *, wait: bool = True, cancel_pending: bool = True) -> None:
        """Tear down dispatcher and process pool.

        ``wait=True`` without ``cancel_pending`` is a graceful stop: the
        dispatcher keeps placing queued tasks until the queue is empty,
        then the pool runs everything it holds before the workers exit --
        the same contract as ``ProcessPoolExecutor.shutdown(wait=True)``.
        """
        if wait and not cancel_pending:
            self._drain_queue()

        if self._stop is not None:
            self._stop.set()
        if self._cv is not None:
            with self._cv:
                self._cv.notify_all()

        if wait and self._dispatcher is not None:
            self._dispatcher.join()

        if cancel_pending:
            self._cancel_queued("Task canceled before dispatch.")

        if self._worker_pool is not None:
            self._worker_pool.shutdown(wait=wait, cancel_futures=cancel_pending)

    def _drain_queue(self) -> None:
        """Block until the dispatcher has placed every queued task."""
        dispatcher = self._dispatcher
        if dispatcher is None or not dispatcher.is_alive():
            return
        if threading.current_thread() is dispatcher:
            return
        while len(self._q) > 0 and dispatcher.is_alive():
            if self._stop is not None and self._stop.is_set():
                return
            with self._cv:
                self._cv.notify_all()
            dispatcher.join(timeout=0.05)

    def _cancel_queued(self, reason: str, *, status: FutureStatus = FutureStatus.CANCELLED) -> None:
        """Fail every queued-but-undispatched future with *reason* and empty the queue."""
        with self._q.atomic("cancel"):
            for _, (_, _, kwargs) in self._q.items():
                fut = kwargs.get("fut")
                if fut is None:
                    continue
                fut.exception = RuntimeError(reason)
                fut.result = None
                fut.status = status
            self._q.clear()

    def _validate_process_task(
        self,
        task: Callable[..., Any],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> None:
        """Verify that task, args, and kwargs are picklable."""
        try:
            pickle.dumps((task, args, kwargs))
        except Exception as exc:
            raise TypeError(
                "Process pool tasks must be top-level picklable callables with picklable args/kwargs."
            ) from exc

    def _queue_submit(self, task: Callable[..., Any], *args, **kwargs) -> ProcessPackageFuture:
        """Internal: enqueue callable into the task queue."""
        if self.status != TaskForceStatus.RUNNING:
            raise RuntimeError("TaskForce must be running before submitting tasks.")

        self._validate_process_task(task, args, kwargs)

        fut = ProcessPackageFuture(
            taskforce_id=self.global_id,
            policy_id=self.policy_id,
        )

        with self._cv:
            with self._q.atomic():
                kwargs["task"] = task
                kwargs["fut"] = fut
                self._q[fut.global_id] = (_process_runner, args, kwargs)
            self._cv.notify()

        return fut

    def imap(self, tasks: Iterable[Callable[[], Any]]) -> Iterable[Any]:
        """Submit an iterable of zero-arg callables, yielding the futures in submission order."""
        for f in tasks:
            yield self._queue_submit(f)

    def submit(
        self,
        tasks: Iterable[Callable[[], Any]],
        wait: bool = False,
    ) -> GroupFuture | Any:
        """Batch submit zero-arg callables.

        Returns the future itself (single) or a hollow GroupFuture (multiple)
        when *wait* is False.  When *wait* is True, blocks and returns values.
        """
        tasks = list(tasks)

        futures: list[ProcessPackageFuture] = []
        for task in tasks:
            fut = self._queue_submit(task)
            fut.taskforce_id = self.global_id
            futures.append(fut)

        if len(futures) == 1:
            single = futures[0]
            if wait:
                # ``wait=True`` returns a bare value and no handle, so the
                # future would otherwise stay in the bank forever.
                try:
                    return single.wait(None)
                finally:
                    single.release()
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
        try:
            return gf.wait(None)
        finally:
            gf.release()

    def _loop(self):
        """Continuously dispatch tasks from queue to the worker pool."""
        cv = self._cv
        stop = self._stop
        slots = self._submit_slots

        while not stop.is_set():
            with cv:
                while not stop.is_set() and len(self._q) == 0:
                    cv.wait(timeout=0.1)
                if stop.is_set():
                    break
                _, item = self._q.pop_next()
                runner, args, kwargs = item

            while not stop.is_set() and not slots.acquire(timeout=0.1):
                pass
            if stop.is_set():
                with self._q.atomic():
                    self._q[kwargs["fut"].global_id] = (runner, args, kwargs)
                break

            fut = kwargs["fut"]
            task = kwargs["task"]
            process_kwargs = {k: v for k, v in kwargs.items() if k not in {"task", "fut"}}
            fut.status = FutureStatus.RUNNING
            try:
                native = self._place(runner, task, args, process_kwargs)
            except BrokenProcessPool as exc:
                # A worker died and the pool could not be rebuilt; the
                # taskforce cannot make progress any more.
                slots.release()
                self._crash(fut, exc)
                break
            except Exception as exc:
                # Anything else (executor already shut down, ...) fails
                # this task only; the dispatcher keeps serving the queue.
                slots.release()
                fut.exception = exc
                fut.result = None
                fut.status = FutureStatus.ERROR
                continue
            fut.native_future = native
            native.add_done_callback(lambda _f: slots.release())

    def _place(self, runner, task, args, process_kwargs):
        """Submit to the executor, rebuilding it once if a worker died.

        A :class:`BrokenProcessPool` means a worker process vanished; the
        executor has already failed its in-flight futures. The task we are
        placing has *not* run, so it is retried on a fresh executor rather
        than failed. Raises :class:`BrokenProcessPool` only if the fresh
        executor is unusable as well.
        """
        try:
            return self._worker_pool.submit(runner, task, args, process_kwargs)
        except BrokenProcessPool as exc:
            _log.warning(
                "Process pool taskforce %s: worker died (%s); rebuilding the pool.",
                self.global_id,
                exc,
            )
            old = self._worker_pool
            try:
                fresh = self._new_pool()
            except Exception as rebuild_exc:  # pragma: no cover - resource exhaustion
                raise BrokenProcessPool(
                    f"could not rebuild the worker pool after a worker died: {rebuild_exc!r}"
                ) from rebuild_exc
            self._worker_pool = fresh
            try:
                old.shutdown(wait=False, cancel_futures=True)
            except Exception:
                pass
            return fresh.submit(runner, task, args, process_kwargs)

    def _crash(self, fut: ProcessPackageFuture, exc: BaseException) -> None:
        """Flip to ``CRASHED``: fail *fut* and every queued task, stop dispatching."""
        _log.error(
            "Process pool taskforce %s crashed: %s; failing %d queued task(s).",
            self.global_id,
            exc,
            len(self._q),
        )
        fut.exception = exc
        fut.result = None
        fut.status = FutureStatus.ERROR
        self.status = TaskForceStatus.CRASHED
        self._cancel_queued(
            f"Process pool taskforce crashed: {exc!r}",
            status=FutureStatus.ERROR,
        )
        if self._stop is not None:
            self._stop.set()
