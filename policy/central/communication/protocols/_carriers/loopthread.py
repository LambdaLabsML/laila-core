"""Shared event-loop-thread lifecycle for transports.

Every wire-backed transport (stream, datagram, point-to-point, broker,
register carriers and the WebSocket ``tcpip`` protocol) owns a private
asyncio event loop on a daemon thread. The boot and teardown sequence
is identical for all of them and subtle enough that it should exist
exactly once:

``start``
    Create the loop, run the transport's ``_async_start(ready)`` on the
    thread, block the caller until *ready* is set (or the boot raised),
    then keep the loop running with ``run_forever``. A boot failure
    joins the thread and **closes the loop** so a failed ``start()``
    leaks nothing.

``stop``
    Run the transport's ``shutdown()`` coroutine on the loop (it is
    expected to close its endpoints and call
    :func:`cancel_pending_tasks`), stop the loop, join the thread and
    then **close the loop**. Not closing the loop leaks its selector fd
    and self-pipe pair on every lifecycle, which is how long-running
    processes that peer/unpeer repeatedly run out of descriptors.

After ``stop`` the transport's ``_event_loop`` is ``None``; sends must
go through :meth:`_CarrierRPCProtocol._loop_call`, which turns that
into a clear :class:`ConnectionError`.
"""

from __future__ import annotations

import asyncio
import logging
import threading
from collections.abc import Awaitable, Callable
from typing import Any

log = logging.getLogger(__name__)


def start_loop_thread(
    proto: Any,
    async_start: Callable[[threading.Event], Awaitable[None]],
    *,
    ready_timeout: float,
) -> None:
    """Boot *proto*'s private loop thread and block until it is ready.

    Sets ``proto._event_loop`` / ``proto._loop_thread``. Re-raises the
    exception from *async_start* in the caller's thread (with the loop
    already closed) if the boot failed.
    """
    loop = asyncio.new_event_loop()
    proto._event_loop = loop
    ready = threading.Event()
    boot: dict[str, BaseException] = {}

    def _run_loop() -> None:
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(async_start(ready))
        except BaseException as exc:
            boot["error"] = exc
            ready.set()
            return
        loop.run_forever()

    thread = threading.Thread(target=_run_loop, daemon=True, name=f"{type(proto).__name__}-loop")
    proto._loop_thread = thread
    thread.start()
    ready.wait(timeout=ready_timeout)
    if "error" in boot:
        thread.join(timeout=ready_timeout)
        proto._loop_thread = None
        close_loop(proto)
        raise boot["error"]


def stop_loop_thread(
    proto: Any,
    shutdown: Callable[[], Awaitable[None]] | None,
    *,
    timeout: float = 5.0,
) -> None:
    """Run *shutdown* on *proto*'s loop, stop it, join the thread, close the loop.

    Idempotent and best-effort: a shutdown coroutine that raises or
    overruns *timeout* is logged at debug and the loop is still stopped.
    """
    loop = proto._event_loop
    thread = proto._loop_thread
    if loop is not None and not loop.is_closed() and loop.is_running():
        if shutdown is not None:
            try:
                fut = asyncio.run_coroutine_threadsafe(shutdown(), loop)
                fut.result(timeout=timeout)
            except Exception:
                log.debug("%s shutdown coroutine failed", type(proto).__name__, exc_info=True)
        try:
            loop.call_soon_threadsafe(loop.stop)
        except RuntimeError:
            pass
    if thread is not None:
        thread.join(timeout=timeout)
    proto._loop_thread = None
    close_loop(proto)


def close_loop(proto: Any) -> None:
    """Close *proto*'s loop if its thread has exited; always drop the reference."""
    loop = proto._event_loop
    proto._event_loop = None
    if loop is None or loop.is_closed():
        return
    if loop.is_running():
        # The loop thread did not exit within the join timeout. Closing a
        # running loop raises; leave it to die with its daemon thread.
        log.warning("%s event loop still running at close; leaking it", type(proto).__name__)
        return
    try:
        loop.close()
    except Exception:
        log.debug("closing %s event loop failed", type(proto).__name__, exc_info=True)


async def cancel_pending_tasks() -> None:
    """Cancel every other task on the current loop and wait for them to finish.

    Call at the end of a transport's shutdown coroutine so no receive
    loop, writer task or retransmit timer survives into ``loop.stop()``
    (a task destroyed while pending logs an asyncio error on close).
    """
    loop = asyncio.get_running_loop()
    current = asyncio.current_task()
    pending = [task for task in asyncio.all_tasks(loop) if task is not current]
    for task in pending:
        task.cancel()
    if pending:
        await asyncio.gather(*pending, return_exceptions=True)
