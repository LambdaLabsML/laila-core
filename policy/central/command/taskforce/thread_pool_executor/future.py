"""Pydantic-wrapped ``concurrent.futures.Future`` with Laila status lifecycle."""

from __future__ import annotations

import asyncio
import time
from concurrent.futures import Future as _ConcurrentFuture
from concurrent.futures import TimeoutError as FutureTimeoutError
from typing import Any

from pydantic import PrivateAttr

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

    _native_future: _ConcurrentFuture | None = PrivateAttr(default=None)

    def model_post_init(self, __context: Any) -> None:
        """Register with the active local policy and attach done-callback if native future exists."""
        self._setup_default_callbacks()
        from ...... import _get_active_local_policy

        policy = _get_active_local_policy()
        policy.central.command._register_future_with_active_guarantees(self)
        policy.future_bank[self.global_id] = self
        if self._native_future is not None:
            self._add_default_concurrent_future_done_callback()
        try:
            from ......logger import get_logger

            get_logger().record_future_created(self)
        except Exception:
            pass

    @property
    def native_future(self) -> _ConcurrentFuture:
        """Return the underlying ``concurrent.futures.Future``."""
        return self._native_future

    @native_future.setter
    def native_future(self, native_future: _ConcurrentFuture) -> None:
        """Set the underlying native future (one-shot; raises on reassignment)."""
        if self._native_future is not None:
            raise RuntimeError("Native future already set.")
        self._native_future = native_future
        self._add_default_concurrent_future_done_callback()

    def _add_default_concurrent_future_done_callback(self) -> None:
        """Attach a done-callback that syncs native future outcome to Laila status."""

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

    def wait(self, timeout: float | None = None) -> Any:
        """Block until the future completes or *timeout* seconds elapse.

        Raises
        ------
        LoopBlockingWaitError
            If called from a thread that owns an async event loop.
        """
        from ...schema.exceptions import _check_not_loop_thread

        _check_not_loop_thread()

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
        """Await the native future or poll until a terminal status is reached."""

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
