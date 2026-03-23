"""Base schema for the central command sub-system."""

from typing import Callable, Any, Iterable, List, Union, Tuple, Optional
from pydantic import Field, PrivateAttr, ConfigDict
from typing import Dict
import threading

from .future.future import Future
from .....atomic.definitions.identifiable_object import _LAILA_IDENTIFIABLE_OBJECT
from .....macros.strings import _CENTRAL_COMMAND_SCOPE

class _LAILA_IDENTIFIABLE_CENTRAL_COMMAND(_LAILA_IDENTIFIABLE_OBJECT):
    """Central command that manages task-forces, future submission, and guarantees."""

    _scopes: list[str] = PrivateAttr(default_factory=lambda: list([_CENTRAL_COMMAND_SCOPE]))


    taskforces: Dict[str, Any] = Field(default_factory=dict)
    alpha_taskforce: Optional[str] = None
    policy_id: Optional[_LAILA_IDENTIFIABLE_OBJECT | str] = None
    _guarantee_local: threading.local = PrivateAttr(default_factory=threading.local)


    def model_post_init(self, __context: Any) -> None:
        """Create a default task-force and set it as alpha if none provided."""
        if len(self.taskforces) == 0:
            from .....macros.defaults import DefaultTaskForce
            taskforce = DefaultTaskForce(policy_id=self.policy_id)
            self.taskforces[taskforce.global_id] = taskforce

        if self.alpha_taskforce is None:
            self.alpha_taskforce = next(iter(self.taskforces))
        
        return self


    def add_taskforce(
        self, 
        taskforce: Any,
    ):
        """Register a task-force with this central command.

        Parameters
        ----------
        taskforce : _LAILA_IDENTIFIABLE_TASK_FORCE
            The task-force instance to register.
        """
        self.taskforces[taskforce.global_id] = taskforce

    def _guarantee_stack(self) -> list[Dict[str, Future]]:
        """Return the thread-local guarantee scope stack, creating it if needed."""
        stack = getattr(self._guarantee_local, "stack", None)
        if stack is None:
            stack = []
            self._guarantee_local.stack = stack
        return stack

    def _guarantee_enter(self) -> None:
        """Push a new empty scope onto the guarantee stack."""
        self._guarantee_stack().append({})

    def _guarantee_exit(self) -> list[Future]:
        """Pop the current guarantee scope and return its futures."""
        stack = self._guarantee_stack()
        if not stack:
            return []
        return list(stack.pop().values())

    def _register_future_with_active_guarantees(self, future: Any) -> None:
        """Record *future* in every open guarantee scope on this thread."""
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
        taskforce_id: Optional[str] = None
    ) -> Union[Future, List[Any], Any]:
        """Submit tasks to a task-force for execution.

        Parameters
        ----------
        tasks : Iterable[Callable[[], Any]]
            Zero-arg callables to execute.
        wait : bool, optional
            If ``True``, block until all tasks complete and return results.
        taskforce_id : str, optional
            Target task-force; defaults to the alpha task-force.

        Returns
        -------
        Future or list[Any] or Any
            A future (or group future) when *wait* is ``False``, otherwise
            the return value(s).
        """
        if taskforce_id is None:
            taskforce_id = self.alpha_taskforce
    
        return self.taskforces[taskforce_id].submit(
            tasks = tasks,
            wait = wait,
        )

    
    def shutdown(
        self, 
        wait: bool = True, 
        cancel_pending: bool = False
    ) -> None:
        """Shut down all registered task-forces.

        Parameters
        ----------
        wait : bool, optional
            Block until workers finish.
        cancel_pending : bool, optional
            Cancel queued but un-started tasks.
        """
        for tf in self.taskforces.values():
            tf.shutdown(
                wait = wait,
                cancel_pending = cancel_pending
            )

    def __await__(self):
        """Async awaiting is not yet supported."""
        raise NotImplementedError

    