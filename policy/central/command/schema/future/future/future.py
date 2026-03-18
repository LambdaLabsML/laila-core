from __future__ import annotations
from typing import Optional, Callable, Any, Dict, List
from pydantic import ConfigDict, Field, PrivateAttr


from .future_status import FutureStatus
from .future_identity import _LAILA_IDENTIFIABLE_FUTURE
from .......utils.decorators.synchronized import synchronized


class Future(_LAILA_IDENTIFIABLE_FUTURE):
    """
    Abstract Future base class with identity, outcome, and callbacks.

    Parameters
    ----------
    future_identity
        Identity metadata for this Future instance.
    """

    _status: FutureStatus = PrivateAttr(default=FutureStatus.NOT_STARTED)
    _return_value: Any = PrivateAttr(default=None)
    _exception: Optional[Exception] = PrivateAttr(default=None)
    _timeout_ms: int = PrivateAttr(default=100)

    model_config = ConfigDict(arbitrary_types_allowed=True)


    _default_callbacks: Dict[FutureStatus, Callable[..., Any]] = PrivateAttr(default_factory=dict)
    callbacks: Dict[FutureStatus, Callable[..., Any]] = Field(default_factory=dict)

    def model_post_init(self, __context: Any) -> None:
        self._setup_default_callbacks()
        from ....... import get_active_policy
        get_active_policy().central.command._register_future_with_active_guarantees(self)


    def _setup_default_callbacks(self) -> None:
        self._default_callbacks[FutureStatus.ERROR] = lambda f: setattr(f, "status", FutureStatus.ERROR)
        self._default_callbacks[FutureStatus.CANCELLED] = lambda f: setattr(f, "status", FutureStatus.CANCELLED)
        self._default_callbacks[FutureStatus.NOT_STARTED] = lambda f: setattr(f, "status", FutureStatus.NOT_STARTED)
        self._default_callbacks[FutureStatus.RUNNING] = lambda f: setattr(f, "status", FutureStatus.RUNNING)
        self._default_callbacks[FutureStatus.POLL_TIMEOUT] = lambda f: setattr(f, "status", FutureStatus.POLL_TIMEOUT)
        self._default_callbacks[FutureStatus.UNKNOWN] = lambda f: setattr(f, "status", FutureStatus.UNKNOWN)
        self._default_callbacks[FutureStatus.FINISHED] = lambda f: setattr(f, "status", FutureStatus.FINISHED)

    
    @property
    @synchronized
    def status(self) -> FutureStatus:
        """
        Return the current status code for this Future.
        """
        return self._status

    
    @status.setter
    @synchronized
    def status(self, status: FutureStatus) -> None:
        """
        Set the current status code for this Future.
        """
        self._status = status

    
    @property
    def result(self) -> Any:
        """
        Return the current result value. Releases the lock before waiting to
        avoid deadlock with the done callback that sets the result.
        """
        with self.atomic():
            if self._status in [FutureStatus.ERROR, FutureStatus.CANCELLED]:
                raise self._exception
            if self._status == FutureStatus.FINISHED:
                return self._return_value
        self.wait(timeout=None)
        with self.atomic():
            if self._status in [FutureStatus.ERROR, FutureStatus.CANCELLED]:
                raise self._exception
            return self._return_value
    
    
    @result.setter
    @synchronized
    def result(self, result: Any) -> None:
        """
        Set the result value.
        """

        self._return_value = result

    @property
    def exception(self) -> Optional[Exception]:
        """
        Return the current exception value.
        """
        return self._exception

    @exception.setter
    @synchronized
    def exception(self, exception: Optional[Exception]) -> None:
        """
        Set the exception value.
        """
        self._exception = exception

    def add_callback(self, status: FutureStatus, fn: Callable[["Future"], Any]) -> None:
        """
        Register a callback for a specific status transition.
        """
        self.callbacks[status] = fn
    
    def remove_callback(self, status: FutureStatus, fn: Callable[["Future"], Any]) -> None:
        """
        Remove a callback for a specific status.
        """
        self.callbacks[status] = None

    def clear_callbacks(self, status: FutureStatus) -> None:
        """
        Clear the callback for a specific status.
        """
        self._callbacks[status] = None
    
    def clear_all_callbacks(self) -> None:
        """
        Clear all registered callbacks.
        """
        self._callbacks.clear()
    
    #TODO: This needs to go through the central command.
    def trigger_callback(self, status: FutureStatus) -> None:
        """
        Trigger the callback for the given status, if present.
        """
        fn = self._callbacks[status]
        if fn is not None:
            fn(self.result)



    def finished(self) -> bool:
        """
        Return True if the Future has finished successfully.
        """
        return self.status in [FutureStatus.FINISHED]
    def cancelled(self) -> bool:
        """
        Return True if the Future was cancelled.
        """
        return self.status in [FutureStatus.CANCELLED]
    def error(self) -> bool:
        """
        Return True if the Future finished with an error.
        """
        return self.status in [FutureStatus.ERROR]
    def not_started(self) -> bool:
        """
        Return True if the Future has not started.
        """
        return self.status in [FutureStatus.NOT_STARTED]
    def running(self) -> bool:
        """
        Return True if the Future is currently running.
        """
        return self.status in [FutureStatus.RUNNING]

