from __future__ import annotations
from typing import Optional, Any, Callable
from concurrent.futures import Future as _ConcurrentFuture, TimeoutError as FutureTimeoutError
from pydantic import PrivateAttr
import time
import asyncio

from ...schema.future.future.future import Future
from ...schema.future.future.future_status import FutureStatus


class ConcurrentPackageFuture(Future):
    """
    Pydantic v2 wrapper around concurrent.futures.Future with lifecycle + introspection.

    Status lifecycle:
      - NOT_STARTED  → initial state
      - RUNNING      → after set_running_or_notify_cancel()
      - FINISHED     → after successful result()
      - ERROR        → after exception set or raised
    """

    _native_future: Optional[_ConcurrentFuture] = PrivateAttr(default=None)


    def model_post_init(self, __context: Any) -> None:
        self._setup_default_callbacks()
        from ...... import get_active_policy
        get_active_policy().central.command._register_future_with_active_guarantees(self)
        if self._native_future is not None:
            self._add_default_concurrent_future_done_callback()


    @property
    def native_future(self) -> _ConcurrentFuture:
        return self._native_future


    @native_future.setter
    def native_future(self, native_future: _ConcurrentFuture) -> None:
        if self._native_future is not None:
            raise RuntimeError("Native future already set.")
        self._native_future = native_future
        self._add_default_concurrent_future_done_callback()


    def _add_default_concurrent_future_done_callback(self) -> None:

        def _default_done_callback(n_fut: _ConcurrentFuture) -> None:
            if n_fut.cancelled():
                self.result = None
                self.exception = None
                self._default_callbacks[FutureStatus.CANCELLED](self)
            elif n_fut.exception() is not None:
                self.exception = n_fut.exception()
                self.result = None
                self._default_callbacks[FutureStatus.ERROR](self)
            else:
                self.exception = None
                self.result = n_fut.result()
                self._default_callbacks[FutureStatus.FINISHED](self)

        self._native_future.add_done_callback(_default_done_callback)



    def wait(self, timeout: Optional[float] = None) -> Any:
        deadline = None if timeout is None else time.monotonic() + timeout
        poll_interval_s = 0.01

        while True:
            with self.atomic():
                n_fut = self._native_future
                status = self._status
                exc = self._exception
                value = self._return_value

            if n_fut is not None:
                remaining = None if deadline is None else max(0.0, deadline - time.monotonic())
                return n_fut.result(remaining)

            if status == FutureStatus.FINISHED:
                return value
            if status in [FutureStatus.ERROR, FutureStatus.CANCELLED]:
                if exc is not None:
                    raise exc
                raise RuntimeError(f"Future ended with status={status} and no exception.")

            if deadline is not None and time.monotonic() >= deadline:
                self._default_callbacks[FutureStatus.POLL_TIMEOUT](self)
                raise FutureTimeoutError()

            time.sleep(poll_interval_s)


    def __await__(self):
        async def _await_native_or_terminal():
            poll_interval_s = 0.01

            while True:
                with self.atomic():
                    n_fut = self._native_future
                    status = self._status
                    exc = self._exception
                    value = self._return_value

                if n_fut is not None:
                    return await asyncio.wrap_future(n_fut)

                if status == FutureStatus.FINISHED:
                    return value
                if status in [FutureStatus.ERROR, FutureStatus.CANCELLED]:
                    if exc is not None:
                        raise exc
                    raise RuntimeError(f"Future ended with status={status} and no exception.")

                await asyncio.sleep(poll_interval_s)

        return _await_native_or_terminal().__await__()
