/** Pydantic-wrapped ``concurrent.futures.Future`` with Laila status lifecycle. */
import { RuntimeError, TimeoutError as FutureTimeoutError } from "../../../../../_compat/errors.js";
import { lazy, register } from "../../../../../_compat/lazy.js";
import * as asyncio from "../../../../../_compat/asyncio.js";
import { with_ } from "../../../../../_compat/contextlib.js";
import { PrivateAttr, define_private } from "../../../../../_compat/pydantic.js";
import { BlockingNotPossibleError, Event } from "../../../../../_compat/threading.js";
import * as time from "../../../../../_compat/time.js";
import { Future } from "../../schema/future/future/future.js";
import { FutureStatus } from "../../schema/future/future/future_status.js";

const _TERMINAL = [FutureStatus.FINISHED, FutureStatus.ERROR, FutureStatus.CANCELLED];

/**
 * Pydantic v2 wrapper around ``concurrent.futures.Future`` with lifecycle +
 * introspection.
 *
 * Status lifecycle:
 *   - NOT_STARTED  -> initial state
 *   - RUNNING      -> after set_running_or_notify_cancel()
 *   - FINISHED     -> after successful result()
 *   - ERROR        -> after exception set or raised
 */
export class ConcurrentPackageFuture extends Future {
  static {
    define_private(this, {
      _native_future: PrivateAttr({ default: null }),
    });
  }

  /**
   * Apply identity, register with the active local policy and attach the
   * done-callback if a native future was supplied.
   */
  model_post_init(_context) {
    super.model_post_init(_context);
    if (this._native_future !== null && this._native_future !== undefined) {
      this._add_default_concurrent_future_done_callback();
    }
  }

  /** Return the underlying ``concurrent.futures.Future``. */
  get native_future() {
    return this._native_future;
  }

  /** Set the underlying native future (one-shot; raises on reassignment). */
  set native_future(native_future) {
    if (this._native_future !== null && this._native_future !== undefined) {
      throw new RuntimeError("Native future already set.");
    }
    this._native_future = native_future;
    this._add_default_concurrent_future_done_callback();
  }

  /** Attach a done-callback that syncs native future outcome to Laila status. */
  _add_default_concurrent_future_done_callback() {
    const _default_done_callback = (n_fut) => {
      if (n_fut.cancelled()) {
        this.result = null;
        this.exception = null;
        this._default_callbacks.get(FutureStatus.CANCELLED)(this);
      } else if (n_fut.exception() !== null && n_fut.exception() !== undefined) {
        this.exception = n_fut.exception();
        this.result = null;
        this._default_callbacks.get(FutureStatus.ERROR)(this);
      } else {
        this.exception = null;
        this.result = n_fut.result();
        this._default_callbacks.get(FutureStatus.FINISHED)(this);
      }
    };

    this._native_future.add_done_callback(_default_done_callback);
  }

  /**
   * Block until the future completes or *timeout* seconds elapse.
   *
   * @throws {LoopBlockingWaitError} If called from a thread that owns an
   *   async event loop (or, in Node, from a context where a synchronous
   *   wait can never be satisfied -- inside a microtask).
   */
  wait(timeout = null) {
    const { _check_not_loop_thread, LoopBlockingWaitError } = lazy("laila.policy.central.command.schema.exceptions");
    const { park_sync } = lazy("laila.policy.central.command.schema.parking");

    _check_not_loop_thread();
    try {
      if (this._is_terminal()) return this._wait_impl(0.0);
      return park_sync((t) => this._wait_impl(t), timeout);
    } catch (err) {
      if (err instanceof BlockingNotPossibleError) {
        const e = new LoopBlockingWaitError(
          "Future.wait() called where a blocking wait cannot complete " +
            `(${err.message}). Use \`await fut\` instead of \`.wait()\` from coroutines.`,
        );
        e.__cause__ = err;
        e.cause = err;
        throw e;
      }
      throw err;
    }
  }

  _is_terminal() {
    const [n_fut, status] = with_(this.atomic(), () => [this._native_future, this._status]);
    if (n_fut !== null && n_fut !== undefined) return n_fut.done();
    return status === FutureStatus.FINISHED || status === FutureStatus.ERROR || status === FutureStatus.CANCELLED;
  }

  _snapshot() {
    return with_(this.atomic(), () => [this._native_future, this._status, this._exception, this._return_value]);
  }

  /**
   * Return the value or raise for a terminal *status*; ``null`` marker otherwise.
   *
   * The FINISHED path goes through ``Future._materialize_result`` so a
   * lazily-stored raw value is wrapped into an ``Entry`` exactly once, on
   * first read.
   */
  _outcome(status, exc, _value) {
    if (status === FutureStatus.FINISHED) return this._materialize_result();
    if (exc !== null && exc !== undefined) throw exc;
    throw new RuntimeError(`Future ended with status=${status} and no exception.`);
  }

  /**
   * Fire *fn(this)* once the future reaches any terminal status.
   *
   * Uses ``add_status_callback``, which also fires immediately if the status
   * is already terminal, so there is no lost-wakeup window between the
   * caller's last status check and the subscription. *fn* must be idempotent
   * (it may fire twice in the race window).
   */
  _subscribe_terminal(fn) {
    for (const st of _TERMINAL) this.add_status_callback(st, fn);
  }

  _unsubscribe_terminal(fn) {
    with_(this.atomic(), () => {
      for (const st of _TERMINAL) {
        const bucket = this._status_callbacks.get(st);
        if (bucket && bucket.length) {
          const i = bucket.indexOf(fn);
          if (i >= 0) bucket.splice(i, 1);
        }
      }
    });
  }

  /** Event-driven blocking wait (no polling). */
  _wait_impl(timeout) {
    const deadline = timeout === null || timeout === undefined ? null : time.monotonic() + timeout;

    let [n_fut, status, exc, value] = this._snapshot();
    if (n_fut !== null && n_fut !== undefined) {
      const remaining = deadline === null ? null : Math.max(0.0, deadline - time.monotonic());
      return n_fut.result(remaining);
    }
    if (_TERMINAL.includes(status)) return this._outcome(status, exc, value);

    const done = new Event();

    const _on_terminal = (_f) => {
      done.set();
    };

    this._subscribe_terminal(_on_terminal);
    try {
      const remaining = deadline === null ? null : Math.max(0.0, deadline - time.monotonic());
      if (!done.wait(remaining)) {
        // Native future may have been attached late; check once more.
        [n_fut, status, exc, value] = this._snapshot();
        if (n_fut !== null && n_fut !== undefined) return n_fut.result(0.0);
        if (!_TERMINAL.includes(status)) {
          this._default_callbacks.get(FutureStatus.POLL_TIMEOUT)(this);
          throw new FutureTimeoutError();
        }
      }
    } finally {
      this._unsubscribe_terminal(_on_terminal);
    }

    [n_fut, status, exc, value] = this._snapshot();
    if (n_fut !== null && n_fut !== undefined) return n_fut.result(0.0);
    return this._outcome(status, exc, value);
  }

  /**
   * Await the native future or a terminal status (event-driven, no polling).
   *
   * Parks the current taskforce slot (if any) while pending so nested awaits
   * can never deadlock the scheduler.
   */
  __await__() {
    const { park_async } = lazy("laila.policy.central.command.schema.parking");

    const _await_native_or_terminal = async () => {
      let [n_fut, status, exc, value] = this._snapshot();
      if (n_fut !== null && n_fut !== undefined) return await asyncio.wrap_future(n_fut);
      if (_TERMINAL.includes(status)) return this._outcome(status, exc, value);

      const loop = asyncio.get_running_loop();
      const done = loop.create_future();

      const _set = () => {
        if (!done.done()) done.set_result(null);
      };

      const _on_terminal = (_f) => {
        try {
          loop.call_soon_threadsafe(_set);
        } catch (e) {
          if (!(e instanceof RuntimeError)) throw e;
          // loop closed during shutdown
        }
      };

      this._subscribe_terminal(_on_terminal);
      try {
        await done;
      } finally {
        this._unsubscribe_terminal(_on_terminal);
      }

      [n_fut, status, exc, value] = this._snapshot();
      if (n_fut !== null && n_fut !== undefined) return await asyncio.wrap_future(n_fut);
      return this._outcome(status, exc, value);
    };

    if (this._is_terminal()) return _await_native_or_terminal();
    return park_async(_await_native_or_terminal());
  }
}

register("laila.policy.central.command.taskforce.thread_pool_executor.future", { ConcurrentPackageFuture });
