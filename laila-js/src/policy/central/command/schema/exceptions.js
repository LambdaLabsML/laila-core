/**
 * Exceptions and runtime guards for the central command system.
 *
 * Provides:
 *
 * - ``LoopBlockingWaitError`` -- raised when ``Future.wait()`` is called
 *   from a thread that is currently driving an async taskforce event loop
 *   (which would deadlock the loop on a future the loop itself needs to
 *   advance).
 * - ``NestedCommandSubmitError`` -- raised when a function decorated
 *   ``no_command_submit`` synchronously invokes ``Command.submit``.
 * - ``no_command_submit`` -- decorator that marks a function as forbidden
 *   from calling ``Command.submit`` while it executes; works on both sync and
 *   async callables via a shared ``ContextVar``.
 * - ``_ASYNC_LOOP_THREAD_IDS`` -- module-level set of thread ids that own
 *   an async event loop. Populated/cleared by ``_LoopThread`` in the
 *   async taskforce. Consulted by future ``wait()`` implementations.
 * - ``_NO_SUBMIT_OWNER`` -- ``ContextVar`` carrying the qualname of the
 *   innermost ``no_command_submit`` function currently executing on the
 *   context. Read by ``Command.submit`` to enforce the contract.
 *
 * Node note: there is one JS thread, so "thread" here means a laila
 * execution context (``_compat/contextvars.Context``). A ``_LoopThread``
 * runs its tasks under its own thread identity; code that is *offloaded*
 * (``run_sync`` / ``asyncio.to_thread``) gets a fresh identity, exactly as
 * an executor thread does in CPython.
 */
import { RuntimeError } from "../../../../_compat/errors.js";
import { ContextVar, get_ident } from "../../../../_compat/contextvars.js";
import { register } from "../../../../_compat/lazy.js";
import { Lock, with_lock } from "../../../../_compat/threading.js";
import * as asyncio from "../../../../_compat/asyncio.js";
import { wraps, qualname } from "../../../../_compat/functools.js";
import { _CURRENT_SLOT } from "./parking.js";

/** @type {Set<number>} */
export const _ASYNC_LOOP_THREAD_IDS = new Set();
const _ASYNC_LOOP_THREAD_IDS_LOCK = new Lock();

/** Mark *thread_ident* as owning an async event loop. */
export function _register_async_loop_thread(thread_ident) {
  with_lock(_ASYNC_LOOP_THREAD_IDS_LOCK, () => {
    _ASYNC_LOOP_THREAD_IDS.add(thread_ident);
  });
}

/** Drop *thread_ident* from the async-loop registry. */
export function _unregister_async_loop_thread(thread_ident) {
  with_lock(_ASYNC_LOOP_THREAD_IDS_LOCK, () => {
    _ASYNC_LOOP_THREAD_IDS.delete(thread_ident);
  });
}

/**
 * Raised when ``Future.wait()`` is called from inside an async loop thread.
 *
 * The blocking wait would freeze the loop on a future that needs the
 * same loop to advance. Use ``await fut`` from inside a coroutine instead.
 */
export class LoopBlockingWaitError extends RuntimeError {}

/** Raise ``LoopBlockingWaitError`` if the current thread owns a loop. */
export function _check_not_loop_thread() {
  if (_ASYNC_LOOP_THREAD_IDS.has(get_ident())) {
    throw new LoopBlockingWaitError(
      "Future.wait() called from inside an async taskforce loop thread. " +
        "Use `await fut` instead of `.wait()` from coroutines.",
    );
  }
}

/** @type {ContextVar<string|null>} */
export const _NO_SUBMIT_OWNER = new ContextVar("_no_submit_owner", { default: null });

/** Raised when a ``no_command_submit`` function calls ``cmd.submit``. */
export class NestedCommandSubmitError extends RuntimeError {}

/**
 * Mark *fn* as forbidden from synchronously invoking ``cmd.submit``.
 *
 * Sets a ``ContextVar`` while *fn* runs; ``Command.submit`` reads the
 * var and raises ``NestedCommandSubmitError`` if it is set. Works
 * for both sync and async callables. The contextvar propagates through
 * ``await`` and across ``asyncio.create_task`` boundaries.
 */
export function no_command_submit(fn) {
  const name = qualname(fn);
  if (asyncio.iscoroutinefunction(fn)) {
    const _async_wrap = wraps(fn)(async function _async_wrap(...args) {
      const token = _NO_SUBMIT_OWNER.set(name);
      try {
        return await fn.apply(this, args);
      } finally {
        _NO_SUBMIT_OWNER.reset(token);
      }
    });
    return _async_wrap;
  }
  const _sync_wrap = wraps(fn)(function _sync_wrap(...args) {
    const token = _NO_SUBMIT_OWNER.set(name);
    try {
      return fn.apply(this, args);
    } finally {
      _NO_SUBMIT_OWNER.reset(token);
    }
  });
  return _sync_wrap;
}

/** Raise ``NestedCommandSubmitError`` if ``no_command_submit`` is active. */
export function _check_no_pending_submit_owner() {
  const owner = _NO_SUBMIT_OWNER.get();
  if (owner !== null) {
    throw new NestedCommandSubmitError(
      `\`${owner}\` is decorated \`@no_command_submit\` but called ` +
        "`Command.submit`. Build functions must not nest command " +
        "submissions; use `await` on a future passed in from outside " +
        "the build, or restructure to submit from the orchestrator.",
    );
  }
}

/**
 * Return a coroutine function for *task*.
 *
 * Coroutine functions (including ``functools.partial`` of one) are
 * returned unchanged. Plain sync callables are wrapped in an ``async``
 * shim so the runner sees a uniform awaitable. The shim offloads the sync
 * body to the owning taskforce's sync executor
 * (``PythonAsyncThreadPoolTaskForce.run_sync``) -- or ``asyncio.to_thread``
 * when not running inside a taskforce slot -- so a CPU-bound or blocking
 * body never stalls the event loop. If the body itself returns a coroutine
 * (e.g. a lambda factory ``() => my_async_fn()``), the shim awaits it on the
 * loop so the runner gets the final value rather than an unawaited coroutine.
 *
 * Prefer submitting coroutine functions / ``partial(coro_fn, ...)`` for
 * async work: they skip the thread hop entirely.
 */
export function ensure_coroutine_function(task) {
  if (asyncio.iscoroutinefunction(task)) return task;

  const _wrap = wraps(task)(async function _wrap(...args) {
    const slot = _CURRENT_SLOT.get();
    const tf = slot ? slot.tf : null;
    let out;
    if (tf != null && typeof tf.run_sync === "function") {
      out = await tf.run_sync(task, ...args);
    } else {
      out = await asyncio.to_thread(task, ...args);
    }
    if (asyncio.iscoroutine(out)) out = await out;
    return out;
  });
  _wrap.__coroutinefunction__ = true;
  return _wrap;
}

register("laila.policy.central.command.schema.exceptions", {
  _ASYNC_LOOP_THREAD_IDS,
  _register_async_loop_thread,
  _unregister_async_loop_thread,
  LoopBlockingWaitError,
  _check_not_loop_thread,
  _NO_SUBMIT_OWNER,
  NestedCommandSubmitError,
  no_command_submit,
  _check_no_pending_submit_owner,
  ensure_coroutine_function,
});
