from __future__ import annotations

# NOTE:
# This process-pool backend currently targets top-level picklable callables
# with picklable args/kwargs/results only. It still needs more development
# before live LAILA Entry objects can be handled safely and predictably across
# process boundaries.

import multiprocessing
import pickle
import threading
from concurrent.futures import ProcessPoolExecutor
from typing import Callable, Any, Iterable, Tuple, List, Union, Optional, Dict

from pydantic import Field, PrivateAttr, ConfigDict

from .future import ProcessPackageFuture
from ...schema.future.future.group_future import GroupFuture
from ...schema.future.future.future_status import FutureStatus
from ..status import TaskForceStatus
from ..base import _LAILA_IDENTIFIABLE_TASK_FORCE


def _process_runner(task: Callable[..., Any], args: Tuple[Any, ...], kwargs: Dict[str, Any]) -> Any:
    return task(*args, **kwargs)


class PythonProcessPoolTaskForce(_LAILA_IDENTIFIABLE_TASK_FORCE):
    """
    Process-pool TaskForce implementation.

    This backend mirrors the thread-pool task force, but only accepts
    top-level picklable callables and picklable args/kwargs/results.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    backend: str = Field(default="processes", description="Execution backend (processes only).")
    num_workers: int = Field(default=4, ge=1, description="Number of worker processes.")

    _cv: Optional[threading.Condition] = PrivateAttr(default=None)
    _worker_pool: Optional[ProcessPoolExecutor] = PrivateAttr(default=None)
    _stop: Optional[threading.Event] = PrivateAttr(default=None)
    _dispatcher: Optional[threading.Thread] = PrivateAttr(default=None)
    _submit_slots: Optional[threading.Semaphore] = PrivateAttr(default=None)
    _mp_context: Any = PrivateAttr(default=None)

    def _on_start(self) -> None:
        """Initialize process pool and dispatcher."""
        if self.backend.lower() != "processes":
            raise ValueError("PythonProcessPoolTaskForce supports processes only.")

        self._cv = threading.Condition()
        self._stop = threading.Event()
        self._mp_context = multiprocessing.get_context("spawn")
        self._worker_pool = ProcessPoolExecutor(
            max_workers=self.num_workers,
            mp_context=self._mp_context,
        )
        self._submit_slots = threading.Semaphore(max(1, self.num_workers * 2))
        self._dispatcher = threading.Thread(
            target=self._loop, name="ProcessTaskForce-Dispatcher", daemon=True
        )
        self._dispatcher.start()

    def _on_pause(self) -> None:
        """Pause dispatcher loop without destroying the pool (currently a no-op)."""
        raise NotImplementedError

    def _on_shutdown(self, *, wait: bool = True, cancel_pending: bool = True) -> None:
        """Tear down dispatcher and process pool."""
        if self._stop is not None:
            self._stop.set()
        if self._cv is not None:
            with self._cv:
                self._cv.notify_all()

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

        if self._worker_pool is not None:
            self._worker_pool.shutdown(wait=wait, cancel_futures=cancel_pending)

    def _validate_process_task(
        self,
        task: Callable[..., Any],
        args: Tuple[Any, ...],
        kwargs: Dict[str, Any],
    ) -> None:
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

    def imap(self, tasks: Iterable[Callable[[], Any]]) -> Iterable[ProcessPackageFuture]:
        """Submit an iterable of zero-arg callables, yielding futures in submission order."""
        for f in tasks:
            yield self._queue_submit(f)

    def submit(
        self,
        tasks: Iterable[Callable[[], Any]],
        wait: bool = False,
    ) -> Union[GroupFuture, ProcessPackageFuture, List[Any], Any]:
        """Batch submit zero-arg callables."""
        tasks = list(tasks)

        futures: List[ProcessPackageFuture] = []
        for task in tasks:
            fut = self._queue_submit(task)
            fut.taskforce_id = self.global_id
            futures.append(fut)

        if len(futures) == 1:
            single = futures[0]
            if wait:
                return single.wait(None)
            return single

        gf = GroupFuture(
            taskforce_id=self.global_id,
            policy_id=self.policy_id,
            futures={f.global_id: f for f in futures},
        )

        for f in futures:
            f.future_group_id = gf.global_id

        if not wait:
            return gf
        return gf.wait(None)

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
            try:
                fut.status = FutureStatus.RUNNING
                fut.native_future = self._worker_pool.submit(
                    runner,
                    task,
                    args,
                    process_kwargs,
                )
                fut.native_future.add_done_callback(lambda _f: slots.release())
            except Exception:
                slots.release()
                raise
