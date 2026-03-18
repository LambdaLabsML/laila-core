from __future__ import annotations

import asyncio
from contextlib import suppress
import laila


class gaurantee:
    """Context manager that waits for futures created inside its scope."""

    def __enter__(self):
        laila.get_active_policy().central.command._guarantee_enter()
        return self

    def __exit__(self, exc_type, exc, tb):
        created_inside = laila.get_active_policy().central.command._guarantee_exit()

        wait_errors = []
        for future in created_inside:
            try:
                future.wait(timeout=None)
            except Exception as wait_exc:
                wait_errors.append(wait_exc)

        if exc_type is not None:
            return False

        if wait_errors:
            raise wait_errors[0]

        return False


class async_gaurantee:
    """Async context manager that awaits futures created inside its scope."""

    def __init__(self) -> None:
        self._command = None
        self._scope = None
        self._parent_task = None
        self._watcher_task = None
        self._background_exception = None

    async def __aenter__(self):
        self._command = laila.get_active_policy().central.command
        self._command._guarantee_enter()
        self._scope = self._command._guarantee_stack()[-1]
        self._parent_task = asyncio.current_task()
        self._background_exception = None
        self._watcher_task = asyncio.create_task(self._watch_for_future_errors())
        return self

    async def __aexit__(self, exc_type, exc, tb):
        created_inside = self._command._guarantee_exit()

        if self._watcher_task is not None and not self._watcher_task.done():
            self._watcher_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._watcher_task

        wait_errors = []
        for future in created_inside:
            try:
                if hasattr(future, "__await__"):
                    await future
                else:
                    await asyncio.to_thread(future.wait, None)
            except Exception as wait_exc:
                wait_errors.append(wait_exc)

        if self._background_exception is not None:
            raise self._background_exception

        if exc_type is not None:
            return False

        if wait_errors:
            raise wait_errors[0]

        return False

    async def _watch_for_future_errors(self) -> None:
        watched: dict[str, asyncio.Task] = {}

        while True:
            for future_id, future in list(self._scope.items()):
                if future_id in watched:
                    continue
                watched[future_id] = asyncio.create_task(self._await_future(future))

            if not watched:
                await asyncio.sleep(0.01)
                continue

            done, _ = await asyncio.wait(
                watched.values(),
                timeout=0.01,
                return_when=asyncio.FIRST_COMPLETED,
            )

            for future_id, task in list(watched.items()):
                if task not in done:
                    continue
                watched.pop(future_id, None)
                try:
                    await task
                except Exception as exc:
                    self._background_exception = exc
                    if self._parent_task is not None and not self._parent_task.done():
                        self._parent_task.cancel()
                    return

    async def _await_future(self, future):
        if hasattr(future, "__await__"):
            return await future
        return await asyncio.to_thread(future.wait, None)


guarantee = gaurantee()
guarantee_async = async_gaurantee()
