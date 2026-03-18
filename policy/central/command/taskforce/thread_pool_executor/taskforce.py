from __future__ import annotations
import threading
from concurrent.futures import ThreadPoolExecutor, CancelledError
from typing import Callable, Any, Iterable, Tuple, List, Union, Optional, Dict

from pydantic import Field, PrivateAttr, ConfigDict

from .future import ConcurrentPackageFuture
from ...schema.future.future.group_future import GroupFuture
from ...schema.future.future.future_status import FutureStatus
from ..status import TaskForceStatus
from ..base import _LAILA_IDENTIFIABLE_TASK_FORCE


class PythonThreadPoolTaskForce(_LAILA_IDENTIFIABLE_TASK_FORCE):
    """
    Thread-pool TaskForce implementation.

    Inherits shared queue management, len()/queue_len, and lifecycle surface
    (start, pause, shutdown) from the base `TaskForce`.

    This subclass implements the backend-specific hooks using a
    ThreadPoolExecutor and dispatcher thread to consume items from `_q`.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    backend: str = Field(default="threads", description="Execution backend (threads only).")
    num_workers: int = Field(default=4, ge=1, description="Number of worker threads.")


    # Runtime (Private)
    _cv: Optional[threading.Condition] = PrivateAttr(default=None)
    _worker_pool: Optional[ThreadPoolExecutor] = PrivateAttr(default=None)
    _stop: Optional[threading.Event] = PrivateAttr(default=None)
    _dispatcher: Optional[threading.Thread] = PrivateAttr(default=None)
    _submit_slots: Optional[threading.Semaphore] = PrivateAttr(default=None)

    # =========================================================
    # Lifecycle hooks required by TaskForce
    # =========================================================
        

    def _on_start(self) -> None:
        """Initialize thread pool and dispatcher."""
        if self.backend.lower() != "threads":
            raise ValueError("PythonThreadPoolTaskForce supports threads only.")

        # (Re)create runtime primitives
        self._cv = threading.Condition()
        self._stop = threading.Event()
        self._worker_pool = ThreadPoolExecutor(
            max_workers=self.num_workers, thread_name_prefix="TaskForce"
        )
        # Backpressure: cap in-flight submissions so dispatcher cannot flood executor queue.
        self._submit_slots = threading.Semaphore(max(1, self.num_workers * 2))
        self._dispatcher = threading.Thread(
            target=self._loop, name="TaskForce-Dispatcher", daemon=True
        )
        self._dispatcher.start()

    def _on_pause(self) -> None:
        """Pause dispatcher loop without destroying the pool (currently a no-op)."""
        raise NotImplementedError

    def _on_shutdown(self, *, wait: bool = True, cancel_pending: bool = True) -> None:
        """Tear down dispatcher and thread pool."""
        # Signal dispatcher to stop and wake it up before draining queue.
        if self._stop is not None:
            self._stop.set()
        if self._cv is not None:
            with self._cv:
                self._cv.notify_all()

        # Join dispatcher if requested.
        if wait and self._dispatcher is not None:
            self._dispatcher.join()

        # Optionally cancel pending queue items after dispatcher is stopped/signaled.
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

        # Shutdown the pool.
        if self._worker_pool is not None:
            self._worker_pool.shutdown(wait=wait)

    # =========================================================
    # Task submission and mapping
    # =========================================================

    def _queue_submit(self, task: Callable[..., Any], *args, **kwargs) -> ConcurrentPackageFuture:
        """Internal: enqueue callable into the task queue."""
        if self.status != TaskForceStatus.RUNNING:
            raise RuntimeError("TaskForce must be running before submitting tasks.")

        fut = ConcurrentPackageFuture(
            taskforce_id=self.global_id,
            policy_id=self.policy_id
        )

        with self._cv:
            with self._q.atomic():
                kwargs["task"] = task
                kwargs["fut"] = fut
                self._q[fut.global_id] = (PythonThreadPoolTaskForce._runner, args, kwargs)
            self._cv.notify()

        return fut

    def imap(self, tasks: Iterable[Callable[[], Any]]) -> Iterable[ConcurrentPackageFuture]:
        """Submit an iterable of zero-arg callables, yielding futures in submission order."""
        for f in tasks:
            yield self._queue_submit(f)

    def submit(
        self,
        tasks: Iterable[Callable[[], Any]],
        wait: bool = False,
    ) -> Union[GroupFuture, ConcurrentPackageFuture, List[Any], Any]:
        """Batch submit zero-arg callables."""

        tasks = list(tasks)

        futures: List[ConcurrentPackageFuture] = []

        for task in tasks:
            fut = self._queue_submit(task)
            fut.taskforce_id = self.global_id
            futures.append(fut)


        # Single callable optimization
        if len(futures) == 1:
            single = futures[0]
            if wait:
                try:
                    return single.wait(None)
                except Exception as e:
                    raise 
            else:
                return single

        # Multiple callables
        gf = GroupFuture(
            taskforce_id=self.global_id,
            policy_id=self.policy_id,
            futures={f.global_id: f for f in futures},
        )

        for f in futures:
            f.future_group_id = gf.global_id
            
        if not wait:
            return gf
        else:
            return gf.wait(None)


    # =========================================================
    # Dispatcher loop
    # =========================================================

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
                _, item = self._q.pop_next()  # FIFO
                runner, args, kwargs = item

            while not stop.is_set() and not slots.acquire(timeout=0.1):
                pass
            if stop.is_set():
                with self._q.atomic():
                    self._q[kwargs["fut"].global_id] = (runner, args, kwargs)
                break

            fut = kwargs["fut"]
            try:
                fut.native_future = self._worker_pool.submit(runner, args, kwargs)
                fut.native_future.add_done_callback(lambda _f: slots.release())
            except Exception:
                slots.release()
                raise

    @staticmethod
    def _runner(args: Tuple[Any, ...], kwargs: Dict[str, Any]) -> Any:
        task = kwargs.pop("task")
        fut = kwargs.pop("fut")
        fut.status = FutureStatus.RUNNING
        return task(*args, **kwargs)
        
        
