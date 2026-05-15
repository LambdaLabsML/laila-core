"""Abstract base task-force with lifecycle management and a submission queue."""

from __future__ import annotations
import threading
from typing import Callable, Any, Iterable, List, Union, Tuple, Optional
from pydantic import Field, PrivateAttr, ConfigDict
from .....basics.definitions.cli_capable import CLIExempt, _LAILA_CLI_CAPABLE_CLASS

from .....atomic import AtomicDict
from ..schema.future.future import Future
from .status import TaskForceStatus
from .....macros.strings import _TASK_FORCE_SCOPE
from .....atomic.definitions.locally_atomic_identifiable_object import _LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT
from .....basics.definitions.identifiable_object import _LAILA_IDENTIFIABLE_OBJECT


_LIVE_TASKFORCES: "set[Any]" = set()
_LIVE_TASKFORCES_LOCK = threading.Lock()


def _register_live_taskforce(tf: Any) -> None:
    """Track *tf* in the process-wide live set so ``laila.terminate`` can sweep
    orphan task-forces (those not reachable via ``laila._local_policies``)."""
    with _LIVE_TASKFORCES_LOCK:
        _LIVE_TASKFORCES.add(tf)


def _unregister_live_taskforce(tf: Any) -> None:
    """Remove *tf* from the live set (best-effort; idempotent)."""
    with _LIVE_TASKFORCES_LOCK:
        _LIVE_TASKFORCES.discard(tf)


def _live_taskforces_snapshot() -> List[Any]:
    """Return a snapshot list of currently-registered live task-forces."""
    with _LIVE_TASKFORCES_LOCK:
        return list(_LIVE_TASKFORCES)


class _LAILA_IDENTIFIABLE_TASK_FORCE(_LAILA_CLI_CAPABLE_CLASS, _LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT):
    """Abstract base class for taskforces that manage worker pools and task queues."""

    _scopes: list[str] = PrivateAttr(default_factory=lambda: list([_TASK_FORCE_SCOPE]))

    model_config = ConfigDict(arbitrary_types_allowed=True)

    policy_id: Optional[_LAILA_IDENTIFIABLE_OBJECT | str] = CLIExempt(default=None)

    status: TaskForceStatus = CLIExempt(
        default=TaskForceStatus.NOT_STARTED,
        description="Current lifecycle status of this TaskForce.",
    )

    # ---- Shared runtime state (available to subclasses) ----
    _q: AtomicDict[str, Tuple[Callable[..., Any], tuple, dict]] = PrivateAttr(
        default_factory=AtomicDict
    )


    def model_post_init(self, __context: Any) -> None:
        """Auto-start the taskforce if status is NOT_STARTED."""
        if self.status == TaskForceStatus.NOT_STARTED:
            self.start()

    # ---------- Observability ----------
    @property
    def queue_len(self) -> int:
        """Return the current length of the submission queue."""
        return len(self._q)

    def __len__(self) -> int:
        """
        Length of this TaskForce is defined as the queue length (not worker count).
        """
        return int(self.queue_len)

    # ---------- Public lifecycle ----------
    def start(self) -> None:
        """
        Start underlying resources. Only mark RUNNING after subclass hook succeeds.
        """
        if self.status == TaskForceStatus.RUNNING:
            return
        self._on_start()
        self.status = TaskForceStatus.RUNNING
        _register_live_taskforce(self)

    def pause(self) -> None:
        """
        Quiesce/park the TaskForce without destroying resources.
        Since we only have NOT_STARTED/RUNNING/STOPPED, paused maps to NOT_STARTED.
        """
        if self.status != TaskForceStatus.RUNNING:
            return
        self._on_pause()
        self.status = TaskForceStatus.PAUSED

    def shutdown(self, wait: bool = True, cancel_pending: bool = False) -> None:
        """
        Stop and release underlying resources. Mark STOPPED after subclass hook succeeds.
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
        """Start the taskforce on context-manager entry."""
        self.start()
        return self

    def __exit__(self, exc_type, exc, tb):
        """Shutdown the taskforce on context-manager exit."""
        self.shutdown(wait=True)

    # ---------- Subclass hooks (must implement) ----------
    def _on_start(self) -> None:
        """Allocate/start backend resources (e.g., pools, dispatcher threads)."""
        raise NotImplementedError

    def _on_pause(self) -> None:
        """Quiesce/park backend resources without full teardown."""
        raise NotImplementedError

    def _on_shutdown(self, *, wait: bool, cancel_pending: bool) -> None:
        """Tear down backend resources; honor wait & cancel semantics."""
        raise NotImplementedError

    # ---------- Mapping / submit API (must implement) ----------
    def submit(
        self,
        funcs: Iterable[Callable[[], Any]],
        wait: bool = False,
    ) -> Union[Future, List[Any], Any]:
        """
        Batch submit zero-arg callables.

        Expected returns:
          - wait == False:
              * 1 func -> Future
              * >1 funcs -> Future (e.g. a grouped/aggregate Future)
          - wait == True:
              * 1 func -> result
              * >1 funcs -> [results]
        """
        raise NotImplementedError

    def imap(self, funcs: Iterable[Callable[[], Any]]) -> Iterable[Future]:
        """
        Submit an iterable of zero-arg callables, yielding Future instances
        in submission order.
        """
        raise NotImplementedError
