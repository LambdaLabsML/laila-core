/**
 * ``concurrent.futures`` for the single-threaded runtime (rule E1).
 *
 * Everything Python hands to a ``ThreadPoolExecutor`` / ``asyncio.to_thread``
 * runs here on the *next macrotask* (``pump.hop``) in a fresh execution
 * context, under a permit budget of ``max_workers``. The hop matters: it
 * guarantees that laila code which may block synchronously (``wait()`` on the
 * pump) never runs *inside* a third-party driver callback (``net``, ``ws``,
 * ``pg`` ...), where re-entering the loop would corrupt the driver's state.
 *
 * ``ConcurrentFuture`` mirrors ``concurrent.futures.Future``: blocking
 * ``result(timeout)`` through the pump, thenable for ``await``.
 */
import { blocking_wait } from "./threading.js";
import { hop as _hop } from "./pump.js";
import { Context, current_context } from "./contextvars.js";
import { CancelledError, InvalidStateError, TimeoutError as PyTimeoutError, RuntimeError } from "./errors.js";

const PENDING = "PENDING";
const RUNNING = "RUNNING";
const CANCELLED = "CANCELLED";
const FINISHED = "FINISHED";

/** ``concurrent.futures.Future`` */
export class ConcurrentFuture {
  constructor() {
    this._state = PENDING;
    this._result = undefined;
    this._exception = null;
    this._callbacks = [];
    let resolve, reject;
    this._promise = new Promise((res, rej) => {
      resolve = res;
      reject = rej;
    });
    this._resolve = resolve;
    this._reject = reject;
    // Rule P1: a stored exception must not crash the process as an unhandled
    // rejection; it surfaces only through result()/exception()/await.
    this._promise.catch(() => {});
  }
  cancel() {
    if (this._state === RUNNING || this._state === FINISHED) return false;
    if (this._state === CANCELLED) return true;
    this._state = CANCELLED;
    this._reject(new CancelledError());
    this._invoke_callbacks();
    return true;
  }
  cancelled() {
    return this._state === CANCELLED;
  }
  running() {
    return this._state === RUNNING;
  }
  done() {
    return this._state === CANCELLED || this._state === FINISHED;
  }
  set_running_or_notify_cancel() {
    if (this._state === CANCELLED) return false;
    if (this._state === PENDING) {
      this._state = RUNNING;
      return true;
    }
    throw new RuntimeError(`Future in unexpected state: ${this._state}`);
  }
  set_result(value) {
    if (this.done()) throw new InvalidStateError(`${this._state}: ${this}`);
    this._result = value;
    this._state = FINISHED;
    this._resolve(value);
    this._invoke_callbacks();
  }
  set_exception(exc) {
    if (this.done()) throw new InvalidStateError(`${this._state}: ${this}`);
    this._exception = exc;
    this._state = FINISHED;
    this._reject(exc);
    this._invoke_callbacks();
  }
  _invoke_callbacks() {
    const cbs = this._callbacks;
    this._callbacks = [];
    for (const cb of cbs) {
      try {
        cb(this);
      } catch {
        /* Python logs and continues */
      }
    }
  }
  add_done_callback(fn) {
    if (this.done()) {
      try {
        fn(this);
      } catch {
        /* ignore */
      }
      return;
    }
    this._callbacks.push(fn);
  }
  /** Blocking ``result(timeout=None)``. */
  result(timeout = null) {
    if (!this.done()) {
      const ok = blocking_wait(() => this.done(), timeout);
      if (!ok) throw new PyTimeoutError();
    }
    if (this._state === CANCELLED) throw new CancelledError();
    if (this._exception) throw this._exception;
    return this._result;
  }
  /** Blocking ``exception(timeout=None)``. */
  exception(timeout = null) {
    if (!this.done()) {
      const ok = blocking_wait(() => this.done(), timeout);
      if (!ok) throw new PyTimeoutError();
    }
    if (this._state === CANCELLED) throw new CancelledError();
    return this._exception;
  }
  /** Awaitable (``await fut`` / ``asyncio.wrap_future``). */
  then(onFulfilled, onRejected) {
    return this._promise.then(onFulfilled, onRejected);
  }
  catch(onRejected) {
    return this._promise.catch(onRejected);
  }
  finally(fn) {
    return this._promise.finally(fn);
  }
  get promise() {
    return this._promise;
  }
}

/**
 * Run ``fn(...args)`` on the next macrotask in a fresh context; the returned
 * ConcurrentFuture settles with the (awaited) result.
 */
export function hop(fn, ...args) {
  const fut = new ConcurrentFuture();
  const ctx = new Context(current_context());
  _hop(() => {
    if (!fut.set_running_or_notify_cancel()) return;
    ctx.run(() => {
      let r;
      try {
        r = fn(...args);
      } catch (err) {
        fut.set_exception(err);
        return;
      }
      if (r instanceof Promise) r.then((v) => fut.set_result(v), (e) => fut.set_exception(e));
      else fut.set_result(r);
    });
  });
  return fut;
}

/** ``asyncio.to_thread(fn, *args)`` */
export function to_thread(fn, ...args) {
  return hop(fn, ...args).promise;
}

/** ``concurrent.futures.ThreadPoolExecutor`` */
export class ThreadPoolExecutor {
  constructor(opts = {}) {
    const { max_workers = null, thread_name_prefix = "" } = opts;
    this._max_workers = max_workers ?? 32;
    this._thread_name_prefix = thread_name_prefix;
    this._running = 0;
    this._queue = [];
    this._shutdown = false;
    this._idle_waiters = [];
  }
  submit(fn, ...args) {
    if (this._shutdown) throw new RuntimeError("cannot schedule new futures after shutdown");
    const fut = new ConcurrentFuture();
    this._queue.push({ fn, args, fut, ctx: new Context(current_context(), this._thread_name_prefix || null) });
    this._pump_queue();
    return fut;
  }
  _pump_queue() {
    while (this._running < this._max_workers && this._queue.length) {
      const item = this._queue.shift();
      this._running += 1;
      _hop(() => {
        const { fn, args, fut, ctx } = item;
        const finish = () => {
          this._running -= 1;
          this._pump_queue();
          if (this._running === 0 && this._queue.length === 0) {
            const w = this._idle_waiters;
            this._idle_waiters = [];
            for (const r of w) r();
          }
        };
        if (!fut.set_running_or_notify_cancel()) return finish();
        ctx.run(() => {
          let r;
          try {
            r = fn(...args);
          } catch (err) {
            fut.set_exception(err);
            return finish();
          }
          if (r instanceof Promise) {
            r.then(
              (v) => {
                fut.set_result(v);
                finish();
              },
              (e) => {
                fut.set_exception(e);
                finish();
              },
            );
          } else {
            fut.set_result(r);
            finish();
          }
        });
      });
    }
  }
  /** ``map(fn, *iterables)`` -> array of results (blocking). */
  map(fn, ...iterables) {
    const futs = [];
    const n = Math.min(...iterables.map((it) => [...it].length));
    const arrs = iterables.map((it) => [...it]);
    for (let i = 0; i < n; i++) futs.push(this.submit(fn, ...arrs.map((a) => a[i])));
    return futs.map((f) => f.result());
  }
  /** ``shutdown(wait=True, cancel_futures=False)`` */
  shutdown(opts = {}) {
    const { wait = true, cancel_futures = false } = opts;
    this._shutdown = true;
    if (cancel_futures) {
      for (const item of this._queue) item.fut.cancel();
      this._queue = [];
    }
    if (wait && (this._running > 0 || this._queue.length > 0)) {
      blocking_wait(() => this._running === 0 && this._queue.length === 0, null);
    }
  }
  async shutdown_async(opts = {}) {
    const { cancel_futures = false } = opts;
    this._shutdown = true;
    if (cancel_futures) {
      for (const item of this._queue) item.fut.cancel();
      this._queue = [];
    }
    if (this._running > 0 || this._queue.length > 0) await new Promise((r) => this._idle_waiters.push(r));
  }
  __enter__() {
    return this;
  }
  __exit__() {
    this.shutdown({ wait: true });
    return false;
  }
}

/**
 * ``concurrent.futures.wait(fs, timeout=None, return_when=ALL_COMPLETED)`` -> ``{done, not_done}``
 */
export function wait(fs, opts = {}) {
  const { timeout = null, return_when = "ALL_COMPLETED" } = opts;
  const futs = [...fs];
  const pred = () => {
    const d = futs.filter((f) => f.done());
    if (return_when === "FIRST_COMPLETED") return d.length > 0;
    if (return_when === "FIRST_EXCEPTION") return d.length === futs.length || d.some((f) => !f.cancelled() && f._exception);
    return d.length === futs.length;
  };
  blocking_wait(pred, timeout);
  const done = new Set(futs.filter((f) => f.done()));
  const not_done = new Set(futs.filter((f) => !f.done()));
  return { done, not_done };
}

export const ALL_COMPLETED = "ALL_COMPLETED";
export const FIRST_COMPLETED = "FIRST_COMPLETED";
export const FIRST_EXCEPTION = "FIRST_EXCEPTION";
