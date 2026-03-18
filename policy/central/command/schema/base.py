from typing import Callable, Any, Iterable, List, Union, Tuple, Optional
from pydantic import Field, PrivateAttr, ConfigDict
from typing import Dict
import threading

from .future.future import Future
from .....atomic.definitions.identifiable_object import _LAILA_IDENTIFIABLE_OBJECT
from .....macros.strings import _CENTRAL_COMMAND_SCOPE

class _LAILA_IDENTIFIABLE_CENTRAL_COMMAND(_LAILA_IDENTIFIABLE_OBJECT):
    _scopes: list[str] = PrivateAttr(default_factory=lambda: list([_CENTRAL_COMMAND_SCOPE]))


    taskforces: Dict[str, Any] = Field(default_factory=dict)
    alpha_taskforce: Optional[str] = None
    policy_id: Optional[_LAILA_IDENTIFIABLE_OBJECT | str] = None
    _guarantee_local: threading.local = PrivateAttr(default_factory=threading.local)


    def model_post_init(self, __context: Any) -> None:

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

        self.taskforces[taskforce.global_id] = taskforce

    def _guarantee_stack(self) -> list[Dict[str, Future]]:
        stack = getattr(self._guarantee_local, "stack", None)
        if stack is None:
            stack = []
            self._guarantee_local.stack = stack
        return stack

    def _guarantee_enter(self) -> None:
        self._guarantee_stack().append({})

    def _guarantee_exit(self) -> list[Future]:
        stack = self._guarantee_stack()
        if not stack:
            return []
        return list(stack.pop().values())

    def _register_future_with_active_guarantees(self, future: Any) -> None:
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

        for tf in self.taskforces.values():
            tf.shutdown(
                wait = wait,
                cancel_pending = cancel_pending
            )

    def __await__(self):
        raise NotImplementedError

    