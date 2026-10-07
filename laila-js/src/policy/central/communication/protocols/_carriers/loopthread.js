/**
 * Shared event-loop-thread lifecycle for transports.
 *
 * Every wire-backed transport (stream, datagram, point-to-point, broker,
 * register carriers and the WebSocket ``tcpip`` protocol) owns a private
 * asyncio event loop on a daemon thread. The boot and teardown sequence is
 * identical for all of them and subtle enough that it should exist exactly
 * once:
 *
 * ``start``
 *     Create the loop, run the transport's ``_async_start(ready)`` on the
 *     thread, block the caller until *ready* is set (or the boot raised),
 *     then keep the loop running with ``run_forever``. A boot failure joins
 *     the thread and **closes the loop** so a failed ``start()`` leaks
 *     nothing.
 *
 * ``stop``
 *     Run the transport's ``shutdown()`` coroutine on the loop (it is
 *     expected to close its endpoints and call ``cancel_pending_tasks``),
 *     stop the loop, join the thread and then **close the loop**.
 *
 * After ``stop`` the transport's ``_event_loop`` is ``null``; sends must go
 * through ``_CarrierRPCProtocol._loop_call``, which turns that into a clear
 * ``ConnectionError``.
 *
 * JS mapping
 * ----------
 * Node has one event loop, so the "loop thread" is an ``asyncio._Loop``
 * bound to its own execution context (``asyncio.new_event_loop``). The
 * synchronous ``start_loop_thread`` / ``stop_loop_thread`` block by pumping
 * the Node loop (``threading.Event.wait``), exactly like the Python caller
 * blocks on the real thread; the ``*_async`` variants are the awaitable
 * forms for callers already running inside a microtask.
 */
import * as asyncio from "../../../../../_compat/asyncio.js";
import { RuntimeError, TimeoutError as PyTimeoutError } from "../../../../../_compat/errors.js";
import { getLogger } from "../../../../../_compat/logging.js";
import { Event } from "../../../../../_compat/threading.js";

const log = getLogger("laila.policy.central.communication.protocols._carriers.loopthread");

/** ``threading.Thread``-like handle for a transport's loop context. */
class _LoopThreadHandle {
  constructor(loop, name) {
    this._loop = loop;
    this.name = name;
    this.daemon = true;
  }
  get ident() {
    return this._loop._ctx ? this._loop._ctx.thread_ident : 0;
  }
  is_alive() {
    return this._loop.is_running() && !this._loop.is_closed();
  }
  /** Block until the loop has stopped (or *timeout* seconds elapse). */
  join(timeout = null) {
    if (!this.is_alive()) return;
    const ev = new Event();
    const check = () => {
      if (!this.is_alive()) ev.set();
    };
    this._loop._stop_waiters.push(check);
    check();
    ev.wait(timeout);
  }
  async join_async(timeout = null) {
    if (!this.is_alive()) return;
    await new Promise((res) => {
      let t = null;
      const done = () => {
        if (t) clearTimeout(t);
        res();
      };
      this._loop._stop_waiters.push(done);
      if (timeout !== null && timeout !== undefined) t = setTimeout(done, timeout * 1000);
    });
  }
}

function _boot(proto, async_start) {
  const name = `${proto.constructor.name}-loop`;
  const loop = asyncio.new_event_loop(name);
  proto._event_loop = loop;
  const ready = new Event();
  const boot = {};
  const thread = new _LoopThreadHandle(loop, name);
  proto._loop_thread = thread;
  // ``_run_loop`` on the "thread": set the loop for its context, run the boot
  // coroutine to completion, then ``run_forever`` (the loop stays running
  // until ``stop()``).
  const started = loop.run_forever();
  loop._ctx.run(() => {
    asyncio.set_event_loop(loop);
    const task = asyncio.create_task(() => Promise.resolve(async_start(ready)));
    task.add_done_callback(() => {
      if (task.cancelled()) {
        boot.error = new asyncio.CancelledError();
        ready.set();
        loop.stop();
      } else if (task.exception() !== null) {
        boot.error = task.exception();
        ready.set();
        loop.stop();
      }
    });
  });
  return { loop, ready, boot, thread, started };
}

/**
 * Boot *proto*'s private loop thread and block until it is ready.
 *
 * Sets ``proto._event_loop`` / ``proto._loop_thread``. Re-raises the
 * exception from *async_start* in the caller's thread (with the loop already
 * closed) if the boot failed.
 *
 * @param {any} proto
 * @param {(ready: Event) => Promise<void>} async_start
 * @param {{ready_timeout: number}} opts
 */
export function start_loop_thread(proto, async_start, opts) {
  const { ready_timeout } = opts;
  const { ready, boot, thread } = _boot(proto, async_start);
  ready.wait(ready_timeout);
  if ("error" in boot) {
    thread.join(ready_timeout);
    proto._loop_thread = null;
    close_loop(proto);
    throw boot.error;
  }
}

/** Awaitable form of ``start_loop_thread``. */
export async function start_loop_thread_async(proto, async_start, opts) {
  const { ready_timeout } = opts;
  const { ready, boot, thread } = _boot(proto, async_start);
  await ready.wait_async(ready_timeout);
  if ("error" in boot) {
    await thread.join_async(ready_timeout);
    proto._loop_thread = null;
    close_loop(proto);
    throw boot.error;
  }
}

/**
 * Run *shutdown* on *proto*'s loop, stop it, join the thread, close the loop.
 *
 * Idempotent and best-effort: a shutdown coroutine that raises or overruns
 * *timeout* is logged at debug and the loop is still stopped.
 *
 * @param {any} proto
 * @param {(() => Promise<void>)|null} shutdown
 * @param {{timeout?: number}} [opts]
 */
export function stop_loop_thread(proto, shutdown, opts = {}) {
  const { timeout = 5.0 } = opts;
  const loop = proto._event_loop;
  const thread = proto._loop_thread;
  if (loop !== null && loop !== undefined && !loop.is_closed() && loop.is_running()) {
    if (shutdown !== null && shutdown !== undefined) {
      try {
        const fut = asyncio.run_coroutine_threadsafe(() => shutdown(), loop);
        fut.result(timeout);
      } catch (e) {
        log.debug("%s shutdown coroutine failed", proto.constructor.name, { exc_info: e });
      }
    }
    try {
      loop.call_soon_threadsafe(() => loop.stop());
    } catch (e) {
      if (!(e instanceof RuntimeError)) throw e;
    }
  }
  if (thread !== null && thread !== undefined) thread.join(timeout);
  proto._loop_thread = null;
  close_loop(proto);
}

/** Awaitable form of ``stop_loop_thread``. */
export async function stop_loop_thread_async(proto, shutdown, opts = {}) {
  const { timeout = 5.0 } = opts;
  const loop = proto._event_loop;
  const thread = proto._loop_thread;
  if (loop !== null && loop !== undefined && !loop.is_closed() && loop.is_running()) {
    if (shutdown !== null && shutdown !== undefined) {
      try {
        const fut = asyncio.run_coroutine_threadsafe(() => shutdown(), loop);
        await asyncio.wait_for(fut, timeout);
      } catch (e) {
        if (!(e instanceof PyTimeoutError)) log.debug("%s shutdown coroutine failed", proto.constructor.name, { exc_info: e });
      }
    }
    try {
      loop.call_soon_threadsafe(() => loop.stop());
    } catch (e) {
      if (!(e instanceof RuntimeError)) throw e;
    }
  }
  if (thread !== null && thread !== undefined) await thread.join_async(timeout);
  proto._loop_thread = null;
  close_loop(proto);
}

/** Close *proto*'s loop if its thread has exited; always drop the reference. */
export function close_loop(proto) {
  const loop = proto._event_loop;
  proto._event_loop = null;
  if (loop === null || loop === undefined || loop.is_closed()) return;
  if (loop.is_running()) {
    // The loop thread did not exit within the join timeout. Closing a
    // running loop raises; leave it to die with its daemon thread.
    log.warning("%s event loop still running at close; leaking it", proto.constructor.name);
    return;
  }
  try {
    loop.close();
  } catch (e) {
    log.debug("closing %s event loop failed", proto.constructor.name, { exc_info: e });
  }
}

/**
 * Cancel every other task on the current loop and wait for them to finish.
 *
 * Call at the end of a transport's shutdown coroutine so no receive loop,
 * writer task or retransmit timer survives into ``loop.stop()``.
 */
export async function cancel_pending_tasks() {
  const loop = asyncio.get_running_loop();
  const current = asyncio.current_task();
  const pending = [...asyncio.all_tasks(loop)].filter((t) => t !== current);
  for (const t of pending) t.cancel();
  if (pending.length) await asyncio.gather(...pending, { return_exceptions: true });
}
