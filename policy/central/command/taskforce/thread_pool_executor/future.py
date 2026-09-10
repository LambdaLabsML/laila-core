"""Pydantic-wrapped ``concurrent.futures.Future`` with Laila status lifecycle."""

from __future__ import annotations

import asyncio
import threading
import time
from concurrent.futures import Future as _ConcurrentFuture
from concurrent.futures import TimeoutError as FutureTimeoutError
from typing import Any

from pydantic import PrivateAttr

from ...schema.future.future.future import Future
from ...schema.future.future.future_status import FutureStatus

_TERMINAL = (FutureStatus.FINISHED, FutureStatus.ERROR, FutureStatus.CANCELLED)


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
        """Apply identity, register with the active local policy and attach the
        done-callback if a native future was supplied."""
        super().model_post_init(__context)
        if self._native_future is not None:
            self._add_default_concurrent_future_done_callback()

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
        from ...schema.parking import park_sync

        _check_not_loop_thread()
        if self._is_terminal():
            return self._wait_impl(0.0)
        return park_sync(self._wait_impl, timeout)

    def _is_terminal(self) -> bool:
        with self.atomic():
            n_fut = self._native_future
            status = self._status
        if n_fut is not None:
            return n_fut.done()
        return status in (FutureStatus.FINISHED, FutureStatus.ERROR, FutureStatus.CANCELLED)

    def _snapshot(self):
        with self.atomic():
            return self._native_future, self._status, self._exception, self._return_value

    def _outcome(self, status, exc, value) -> Any:
        """Return the value or raise for a terminal *status*; ``None`` marker otherwise.

        The FINISHED path goes through :meth:`Future._materialize_result`
        so a lazily-stored raw value is wrapped into an :class:`Entry`
        exactly once, on first read.
        """
        if status == FutureStatus.FINISHED:
            return self._materialize_result()
        if exc is not None:
            raise exc
        raise RuntimeError(f"Future ended with status={status} and no exception.")

    def _subscribe_terminal(self, fn) -> None:
        """Fire *fn(self)* once the future reaches any terminal status.

        Uses :meth:`add_status_callback`, which also fires immediately if
        the status is already terminal, so there is no lost-wakeup window
        between the caller's last status check and the subscription. *fn*
        must be idempotent (it may fire twice in the race window).
        """
        for st in _TERMINAL:
            self.add_status_callback(st, fn)

    def _unsubscribe_terminal(self, fn) -> None:
        with self.atomic():
            for st in _TERMINAL:
                bucket = self._status_callbacks.get(st)
                if bucket:
                    try:
                        bucket.remove(fn)
                    except ValueError:
                        pass

    def _wait_impl(self, timeout: float | None) -> Any:
        """Event-driven blocking wait (no polling)."""
        deadline = None if timeout is None else time.monotonic() + timeout

        n_fut, status, exc, value = self._snapshot()
        if n_fut is not None:
            remaining = None if deadline is None else max(0.0, deadline - time.monotonic())
            return n_fut.result(remaining)
        if status in _TERMINAL:
            return self._outcome(status, exc, value)

        done = threading.Event()

        def _on_terminal(_f):
            done.set()

        self._subscribe_terminal(_on_terminal)
        try:
            remaining = None if deadline is None else max(0.0, deadline - time.monotonic())
            if not done.wait(remaining):
                # Native future may have been attached late; check once more.
                n_fut, status, exc, value = self._snapshot()
                if n_fut is not None:
                    return n_fut.result(0.0)
                if status not in _TERMINAL:
                    self._default_callbacks[FutureStatus.POLL_TIMEOUT](self)
                    raise FutureTimeoutError()
        finally:
            self._unsubscribe_terminal(_on_terminal)

        n_fut, status, exc, value = self._snapshot()
        if n_fut is not None:
            return n_fut.result(0.0)
        return self._outcome(status, exc, value)

    def __await__(self):
        """Await the native future or a terminal status (event-driven, no polling).

        Parks the current taskforce slot (if any) while pending so nested
        awaits can never deadlock the scheduler.
        """
        from ...schema.parking import park_async

        async def _await_native_or_terminal():
            n_fut, status, exc, value = self._snapshot()
            if n_fut is not None:
                return await asyncio.wrap_future(n_fut)
            if status in _TERMINAL:
                return self._outcome(status, exc, value)

            loop = asyncio.get_running_loop()
            done: asyncio.Future = loop.create_future()

            def _set():
                if not done.done():
                    done.set_result(None)

            def _on_terminal(_f):
                try:
                    loop.call_soon_threadsafe(_set)
                except RuntimeError:
                    pass  # loop closed during shutdown

            self._subscribe_terminal(_on_terminal)
            try:
                await done
            finally:
                self._unsubscribe_terminal(_on_terminal)

            n_fut, status, exc, value = self._snapshot()
            if n_fut is not None:
                return await asyncio.wrap_future(n_fut)
            return self._outcome(status, exc, value)

        if self._is_terminal():
            return _await_native_or_terminal().__await__()
        return park_async(_await_native_or_terminal()).__await__()
