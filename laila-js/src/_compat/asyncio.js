/**
 * Python ``asyncio`` subset over the Node event loop.
 *
 * - ``Task`` wraps a promise with the ``asyncio.Task`` surface
 *   (``cancel`` / ``cancelled`` / ``done`` / ``result`` / ``exception`` /
 *   ``add_done_callback`` / ``uncancel`` / ``cancelling``). JS cannot
 *   interrupt a coroutine at an arbitrary ``await``, so ``cancel()`` has two
 *   paths: when the body is suspended at a *cooperative* await point
 *   (``sleep``, ``Event.wait``, ``Lock``/``Semaphore``/``Queue`` waits, ...)
 *   the ``CancelledError`` is delivered there -- CPython's ``coro.throw`` --
 *   and the task settles from whatever the body then does (re-raises ->
 *   cancelled, raises something else -> that exception, returns ->
 *   result); otherwise ``cancel()`` settles the *task* with
 *   ``CancelledError`` for everyone awaiting it and the body keeps running to
 *   completion in the background. Either way ``cancelling()`` records the
 *   request, so bodies can also poll ``current_task().cancelling()``.
 * - ``create_task(coro)`` runs the coroutine (a promise or a function
 *   returning one) in a fresh execution context, exactly like a Python task
 *   gets a copy of the current ``contextvars`` context, and sets
 *   ``current_task()`` inside it.
 * - ``sleep`` / ``wait`` / ``wait_for`` / ``gather`` / ``shield`` /
 *   ``to_thread`` / ``run`` have Python semantics (``wait`` returns
 *   ``[done, pending]`` sets, ``wait_for`` raises ``TimeoutError`` ...).
 * - ``Event``, ``Lock``, ``Semaphore``, ``Queue`` are the awaitable
 *   primitives (``await ev.wait()``, ``await q.get()``, ...).
 */
import { Context, ContextVar, current_context } from "./contextvars.js";
import { CancelledError, InvalidStateError, TimeoutError as PyTimeoutError, RuntimeError, TypeError as PyTypeError, ValueError } from "./errors.js";
import { to_thread } from "./executor.js";
import { hop } from "./pump.js";
import { blocking_wait as _blocking_wait } from "./threading.js";
import { Empty as QueueEmpty, Full as QueueFull } from "./queue.js";

export { CancelledError, InvalidStateError, PyTimeoutError as TimeoutError, to_thread, QueueEmpty, QueueFull };

export const FIRST_COMPLETED = "FIRST_COMPLETED";
export const FIRST_EXCEPTION = "FIRST_EXCEPTION";
export const ALL_COMPLETED = "ALL_COMPLETED";

const _current_task = new ContextVar("asyncio.current_task", { default: null });
// The loop "owning" the current execution context (``new_event_loop`` +
// ``set_event_loop`` / a loop-thread context); ``null`` means the default loop.
const _current_loop = new ContextVar("asyncio.current_loop", { default: null });

const PENDING = "PENDING";
const CANCELLED = "CANCELLED";
const FINISHED = "FINISHED";

/** ``asyncio.Future`` -- settable awaitable. */
export class Future {
  constructor() {
    this._state = PENDING;
    this._result = undefined;
    this._exception = null;
    this._callbacks = [];
    this._cancel_requests = 0;
    this._cancel_message = null;
    let resolve, reject;
    this._promise = new Promise((res, rej) => {
      resolve = res;
      reject = rej;
    });
    this._resolve = resolve;
    this._reject = reject;
    this._promise.catch(() => {}); // no unhandled rejection for stored errors
  }
  cancel(msg = null) {
    if (this.done()) return false;
    this._state = CANCELLED;
    this._cancel_message = msg;
    this._reject(new CancelledError(msg ?? ""));
    this._schedule_callbacks();
    return true;
  }
  cancelled() {
    return this._state === CANCELLED;
  }
  done() {
    return this._state !== PENDING;
  }
  result() {
    if (this._state === CANCELLED) throw new CancelledError(this._cancel_message ?? "");
    if (this._state !== FINISHED) throw new InvalidStateError("Result is not set.");
    if (this._exception !== null) throw this._exception;
    return this._result;
  }
  exception() {
    if (this._state === CANCELLED) throw new CancelledError(this._cancel_message ?? "");
    if (this._state !== FINISHED) throw new InvalidStateError("Exception is not set.");
    return this._exception;
  }
  set_result(value) {
    if (this.done()) throw new InvalidStateError(`invalid state`);
    this._result = value;
    this._state = FINISHED;
    this._resolve(value);
    this._schedule_callbacks();
  }
  set_exception(exc) {
    if (this.done()) throw new InvalidStateError(`invalid state`);
    if (exc instanceof CancelledError) {
      this.cancel(exc.message || null);
      return;
    }
    this._exception = exc;
    this._state = FINISHED;
    this._reject(exc);
    this._schedule_callbacks();
  }
  add_done_callback(fn) {
    if (this.done()) {
      queueMicrotask(() => fn(this));
      return;
    }
    this._callbacks.push(fn);
  }
  remove_done_callback(fn) {
    const before = this._callbacks.length;
    this._callbacks = this._callbacks.filter((cb) => cb !== fn);
    return before - this._callbacks.length;
  }
  _schedule_callbacks() {
    const cbs = this._callbacks;
    this._callbacks = [];
    for (const cb of cbs) queueMicrotask(() => cb(this));
  }
  then(onFulfilled, onRejected) {
    return this._promise.then(onFulfilled, onRejected);
  }
  catch(onRejected) {
    return this._promise.catch(onRejected);
  }
  finally(fn) {
    return this._promise.finally(fn);
  }
  get_loop() {
    return _loop;
  }
  __repr__() {
    return `<${this.constructor.name} ${this._state.toLowerCase()}>`;
  }
}

/** ``asyncio.Task`` -- a Future driven by a coroutine. */
export class Task extends Future {
  /**
   * @param {Promise|Function} coro promise, or a function returning one
   * @param {{name?: string, context?: Context}} [opts]
   */
  constructor(coro, opts = {}) {
    super();
    this._name = opts.name ?? `Task-${++Task._counter}`;
    // A task copies the current context (``contextvars.copy_context``) but
    // stays on the *same thread* as its creator.
    this._context = opts.context ?? new Context(current_context(), null, { same_thread: true });
    this._coro_done = false;
    /** Cooperative await points the body is currently suspended at. */
    this._cancel_listeners = new Set();
    // ``asyncio.all_tasks(loop)`` bookkeeping: a task belongs to the loop of
    // the context it was created in (the default loop when none is set).
    this._loop = this._context.get(_current_loop, null) ?? _loop;
    this._loop._tasks.add(this);
    const run = () => {
      _current_task.set(this);
      return typeof coro === "function" ? coro() : coro;
    };
    let p;
    try {
      p = Promise.resolve(this._context.run(run));
    } catch (e) {
      p = Promise.reject(e);
    }
    p.then(
      (v) => {
        this._coro_done = true;
        this._loop._tasks.delete(this);
        if (!this.done()) this.set_result(v);
      },
      (e) => {
        this._coro_done = true;
        this._loop._tasks.delete(this);
        if (!this.done()) {
          if (e instanceof CancelledError) super.cancel(e.message || null);
          else this.set_exception(e);
        }
      },
    );
  }
  static _counter = 0;
  get_name() {
    return this._name;
  }
  set_name(name) {
    this._name = String(name);
  }
  get_coro() {
    return null;
  }
  get_context() {
    return this._context;
  }
  /**
   * Request cancellation. If the body is suspended at a cooperative await
   * point the ``CancelledError`` is thrown in there and the task settles
   * when the body finishes (CPython semantics); otherwise everyone awaiting
   * the task observes ``CancelledError`` right away and the body, which
   * cannot be interrupted, keeps running but can poll ``cancelling()``.
   */
  cancel(msg = null) {
    if (this.done()) return false;
    this._cancel_requests += 1;
    if (this._cancel_listeners.size > 0) {
      this._cancel_message = msg;
      const listeners = [...this._cancel_listeners];
      this._cancel_listeners.clear();
      for (const fn of listeners) fn(new CancelledError(msg ?? ""));
      return true;
    }
    return super.cancel(msg);
  }
  /** Register/unregister the cooperative await point the body is suspended at. */
  _add_cancel_listener(fn) {
    this._cancel_listeners.add(fn);
  }
  _remove_cancel_listener(fn) {
    this._cancel_listeners.delete(fn);
  }
  cancelling() {
    return this._cancel_requests;
  }
  uncancel() {
    if (this._cancel_requests > 0) this._cancel_requests -= 1;
    return this._cancel_requests;
  }
  __repr__() {
    return `<Task ${this._state.toLowerCase()} name='${this._name}'>`;
  }
}

/** ``asyncio.create_task(coro, *, name=None)`` */
export function create_task(coro, opts = {}) {
  return new Task(coro, opts);
}

/** ``asyncio.ensure_future`` */
export function ensure_future(x) {
  if (x instanceof Future) return x;
  return new Task(x);
}

/** ``asyncio.current_task()`` -- ``null`` outside any task. */
export function current_task() {
  return _current_task.get();
}

/**
 * ``asyncio.sleep(delay, result=None)``
 *
 * Cooperative cancellation: when the awaiting task is cancelled the sleep
 * rejects with ``CancelledError`` right away (CPython interrupts the
 * ``await`` at this point) and its timer is cleared, so a cancelled task
 * neither lingers nor keeps the process alive for the remaining delay.
 */
export function sleep(delay, result = null) {
  const ms = Math.max(0, Number(delay) * 1000);
  const task = current_task();
  return new Promise((res, rej) => {
    let settled = false;
    let timer = null;
    const on_cancel = (err) => {
      if (settled) return;
      settled = true;
      if (timer !== null) clearTimeout(timer);
      rej(err);
    };
    const finish = () => {
      if (settled) return;
      settled = true;
      if (task !== null) task._remove_cancel_listener(on_cancel);
      res(result);
    };
    if (task !== null) {
      if (task.cancelled()) {
        on_cancel(new CancelledError(task._cancel_message ?? ""));
        return;
      }
      task._add_cancel_listener(on_cancel);
    }
    if (ms === 0) hop(finish);
    else timer = setTimeout(finish, ms);
  });
}

function _is_awaitable(x) {
  return x !== null && x !== undefined && typeof x.then === "function";
}

function _as_future(x) {
  if (x instanceof Future) return x;
  if (_is_awaitable(x)) {
    const f = new Future();
    x.then(
      (v) => !f.done() && f.set_result(v),
      (e) => !f.done() && f.set_exception(e),
    );
    return f;
  }
  if (typeof x === "function") return new Task(x);
  throw new PyTypeError(`An asyncio.Future, a coroutine or an awaitable is required`);
}

/**
 * ``asyncio.wait(fs, *, timeout=None, return_when=ALL_COMPLETED)`` ->
 * ``[done: Set, pending: Set]``.
 */
export async function wait(fs, opts = {}) {
  const { timeout = null, return_when = ALL_COMPLETED } = opts;
  const futs = [...fs].map(_as_future);
  if (futs.length === 0) throw new ValueError("Set of Tasks/Futures is empty.");
  const done = new Set(futs.filter((f) => f.done()));
  const pending = new Set(futs.filter((f) => !f.done()));
  const satisfied = () => {
    if (pending.size === 0) return true;
    if (return_when === FIRST_COMPLETED) return done.size > 0;
    if (return_when === FIRST_EXCEPTION) {
      for (const f of done) if (f.cancelled() || f.exception() !== null) return true;
    }
    return false;
  };
  if (satisfied()) return [done, pending];
  await new Promise((resolve) => {
    let timer = null;
    let settled = false;
    const finish = () => {
      if (settled) return;
      settled = true;
      if (timer) clearTimeout(timer);
      resolve();
    };
    for (const f of pending) {
      f.add_done_callback(() => {
        if (settled) return;
        pending.delete(f);
        done.add(f);
        if (satisfied()) finish();
      });
    }
    if (timeout !== null && timeout !== undefined) timer = setTimeout(finish, Math.max(0, timeout * 1000));
  });
  return [done, pending];
}

/** ``asyncio.wait_for(aw, timeout)`` -- raises ``TimeoutError`` on expiry. */
export async function wait_for(aw, timeout) {
  const fut = _as_future(aw);
  if (timeout === null || timeout === undefined) return fut;
  let timer;
  const expired = new Promise((_r, rej) => {
    timer = setTimeout(() => rej(new PyTimeoutError()), Math.max(0, timeout * 1000));
  });
  try {
    return await Promise.race([fut, expired]);
  } catch (e) {
    if (e instanceof PyTimeoutError && !fut.done()) fut.cancel();
    throw e;
  } finally {
    clearTimeout(timer);
  }
}

/** ``asyncio.gather(*aws, return_exceptions=False)`` */
export async function gather(...aws) {
  let return_exceptions = false;
  const last = aws[aws.length - 1];
  if (last && typeof last === "object" && !_is_awaitable(last) && !(last instanceof Future) && "return_exceptions" in last) {
    return_exceptions = !!last.return_exceptions;
    aws = aws.slice(0, -1);
  }
  const futs = aws.map(_as_future);
  if (!return_exceptions) return Promise.all(futs.map((f) => f._promise));
  const settled = await Promise.allSettled(futs.map((f) => f._promise));
  return settled.map((s) => (s.status === "fulfilled" ? s.value : s.reason));
}

/** ``asyncio.shield(aw)`` -- cancelling the outer future does not cancel the inner. */
export function shield(aw) {
  const inner = _as_future(aw);
  const outer = new Future();
  inner.add_done_callback(() => {
    if (outer.done()) return;
    if (inner.cancelled()) outer.cancel();
    else if (inner.exception() !== null) outer.set_exception(inner.exception());
    else outer.set_result(inner.result());
  });
  return outer;
}

/** ``asyncio.run(main())`` -- runs ``main`` as a task in a fresh context. */
export function run(main) {
  return new Task(main)._promise;
}

/**
 * ``asyncio.iscoroutine`` -- a *coroutine object*, i.e. a native ``Promise``
 * (what calling an ``async function`` returns). Other awaitables (laila
 * futures, ``asyncio.Future``) are not coroutines, exactly as in Python.
 */
export function iscoroutine(x) {
  return x instanceof Promise;
}
/** ``inspect.isawaitable`` -- anything with a ``then`` (``__await__``). */
export function isawaitable(x) {
  return _is_awaitable(x);
}
/** ``inspect.iscoroutinefunction`` -- an ``async function`` (any flavour). */
export function iscoroutinefunction(fn) {
  return typeof fn === "function" && (fn[Symbol.toStringTag] === "AsyncFunction" || fn.__coroutinefunction__ === true);
}
export function isfuture(x) {
  return x instanceof Future;
}
/** ``asyncio.wrap_future(concurrent_future)`` -> awaitable ``asyncio.Future``. */
export function wrap_future(cf) {
  return _as_future(cf);
}

// --------------------------------------------------------------------------
// Minimal loop handle (``get_event_loop`` / ``get_running_loop``)
// --------------------------------------------------------------------------

/**
 * ``asyncio.AbstractEventLoop`` handle.
 *
 * Node has exactly one event loop, so a Python *loop* maps to an execution
 * context: the default loop is the main context; ``new_event_loop()`` creates
 * a loop bound to a fresh context with its own thread identity (what a
 * ``threading.Thread`` running ``loop.run_forever()`` is in Python). Tasks
 * created from within a loop's context belong to it (``all_tasks(loop)``),
 * and ``call_soon_threadsafe`` / ``run_coroutine_threadsafe`` hop onto it.
 */
export class _Loop {
  constructor(ctx = null) {
    /** Execution context of this loop; ``null`` = the main context. */
    this._ctx = ctx;
    /** @type {Set<Task>} */
    this._tasks = new Set();
    this._default_executor = null;
    this._closed = false;
    this._running = ctx === null;
    this._stop_requested = false;
    /** @type {Array<() => void>} resolvers of ``run_forever`` waiters */
    this._stop_waiters = [];
    this._handle = this;
    if (ctx !== null) ctx.vars.set(_current_loop, this);
  }
  _run_ctx(fn, args) {
    if (this._ctx === null) return fn(...args);
    return this._ctx.run(fn, ...args);
  }
  time() {
    return performance.now() / 1000;
  }
  /** Schedule *fn* on the next loop iteration (microtask) in this loop's context. */
  call_soon(fn, ...args) {
    if (this._closed) throw new RuntimeError("Event loop is closed");
    let cancelled = false;
    queueMicrotask(() => {
      if (!cancelled) this._run_ctx(fn, args);
    });
    return {
      cancel() {
        cancelled = true;
      },
    };
  }
  /**
   * Schedule *fn* from "another thread": a macrotask hop that lands in this
   * loop's context. Works from synchronous code that is pumping the loop.
   */
  call_soon_threadsafe(fn, ...args) {
    if (this._closed) throw new RuntimeError("Event loop is closed");
    let cancelled = false;
    hop(() => {
      if (!cancelled) this._run_ctx(fn, args);
    });
    return {
      cancel() {
        cancelled = true;
      },
    };
  }
  set_default_executor(executor) {
    this._default_executor = executor;
  }
  call_later(delay, fn, ...args) {
    const t = setTimeout(() => this._run_ctx(fn, args), Math.max(0, delay * 1000));
    return { cancel: () => clearTimeout(t) };
  }
  call_at(when, fn, ...args) {
    return this.call_later(when - this.time(), fn, ...args);
  }
  create_task(coro, opts) {
    return this._run_ctx(() => create_task(coro, opts), []);
  }
  create_future() {
    return new Future();
  }
  run_in_executor(executor, fn, ...args) {
    const ex = executor ?? this._default_executor;
    if (ex && typeof ex.submit === "function") return _as_future(ex.submit(fn, ...args));
    return _as_future(to_thread(fn, ...args));
  }
  /** ``loop.run_forever()`` -- resolves once ``stop()`` is called. */
  run_forever() {
    if (this._closed) throw new RuntimeError("Event loop is closed");
    if (this._running && this._ctx !== null) throw new RuntimeError("This event loop is already running");
    this._running = true;
    this._stop_requested = false;
    return new Promise((res) => this._stop_waiters.push(res));
  }
  /** ``loop.run_until_complete(aw)`` -- awaitable in JS. */
  run_until_complete(aw) {
    return this._run_ctx(() => _as_future(aw)._promise, []);
  }
  stop() {
    if (this._ctx === null) return; // the default loop never stops
    this._stop_requested = true;
    this._running = false;
    const ws = this._stop_waiters;
    this._stop_waiters = [];
    for (const w of ws) w();
  }
  close() {
    if (this._running && this._ctx !== null) throw new RuntimeError("Cannot close a running event loop");
    this._closed = true;
  }
  is_running() {
    return this._running;
  }
  is_closed() {
    return this._closed;
  }
  /** ``loop.shutdown_asyncgens()`` -- nothing to do in JS. */
  async shutdown_asyncgens() {}
  async shutdown_default_executor() {}
  /** ``loop.create_datagram_endpoint(protocol_factory, local_addr=..., remote_addr=...)`` */
  create_datagram_endpoint(protocol_factory, opts = {}) {
    return create_datagram_endpoint(protocol_factory, opts);
  }
  /** ``loop.getaddrinfo`` -- resolves to ``[[family, type, proto, canonname, sockaddr], ...]``. */
  async getaddrinfo(host, port) {
    const { lookup } = await import("node:dns/promises");
    const addrs = await lookup(host, { all: true });
    return addrs.map((a) => [a.family === 6 ? 10 : 2, 1, 6, "", [a.address, port]]);
  }
}
const _loop = new _Loop(null);

/** The loop owning the *current* execution context (default loop when none). */
export function get_event_loop() {
  return _current_loop.get() ?? _loop;
}
export function get_running_loop() {
  return _current_loop.get() ?? _loop;
}
/** ``asyncio.new_event_loop()`` -- a loop bound to a fresh execution context. */
export function new_event_loop(name = null) {
  return new _Loop(new Context(current_context(), name ?? "asyncio-loop"));
}
/** ``asyncio.set_event_loop(loop)`` -- binds *loop* to the current context. */
export function set_event_loop(loop) {
  _current_loop.set(loop);
}
/** ``asyncio.all_tasks(loop=None)`` -> Set of pending tasks of *loop*. */
export function all_tasks(loop = null) {
  const l = loop ?? get_running_loop();
  return new Set([...l._tasks].filter((t) => !t.done()));
}
/**
 * ``asyncio.run_coroutine_threadsafe(coro, loop)`` -> a
 * ``concurrent.futures.Future``-like (``.result(timeout)`` blocks by pumping;
 * awaitable). The coroutine runs as a task on *loop*'s context.
 */
export function run_coroutine_threadsafe(coro, loop) {
  const cf = new _ConcurrentFutureLite();
  if (loop._closed) throw new RuntimeError("Event loop is closed");
  loop.call_soon_threadsafe(() => {
    let task;
    try {
      task = create_task(coro);
    } catch (e) {
      if (!cf.done()) cf.set_exception(e);
      return;
    }
    task.add_done_callback(() => {
      if (cf.done()) return;
      if (task.cancelled()) cf.cancel();
      else if (task.exception() !== null) cf.set_exception(task.exception());
      else cf.set_result(task.result());
    });
    cf._task = task;
  });
  return cf;
}

/**
 * ``concurrent.futures.Future`` for ``run_coroutine_threadsafe`` (kept local
 * to avoid an import cycle with ``executor.js``). ``result(timeout)`` blocks
 * by pumping the loop (``threading.blocking_wait``).
 */
class _ConcurrentFutureLite extends Future {
  constructor() {
    super();
    this._task = null;
  }
  cancel(msg = null) {
    if (this._task && !this._task.done()) this._task.cancel(msg);
    return super.cancel(msg);
  }
  result(timeout = null) {
    if (!this.done()) {
      if (timeout === 0) throw new PyTimeoutError();
      const ok = _blocking_wait(() => this.done(), timeout);
      if (!ok) throw new PyTimeoutError();
    }
    return super.result();
  }
  exception(timeout = null) {
    if (!this.done()) {
      const ok = _blocking_wait(() => this.done(), timeout);
      if (!ok) throw new PyTimeoutError();
    }
    return super.exception();
  }
}

// --------------------------------------------------------------------------
// Synchronisation primitives
// --------------------------------------------------------------------------

/** ``asyncio.Event`` */
export class Event {
  constructor() {
    this._flag = false;
    this._waiters = [];
  }
  is_set() {
    return this._flag;
  }
  set() {
    if (this._flag) return;
    this._flag = true;
    const ws = this._waiters;
    this._waiters = [];
    for (const w of ws) w();
  }
  clear() {
    this._flag = false;
  }
  /**
   * ``await event.wait()``. Cancellation-aware: when the current task is
   * cancelled while waiting, the wait rejects with ``CancelledError`` (the
   * cooperative stand-in for Python interrupting the coroutine).
   */
  wait() {
    if (this._flag) return Promise.resolve(true);
    return _cancellable(this, new Promise((res) => this._waiters.push(() => res(true))));
  }
}

/**
 * Make a pending wait reject with ``CancelledError`` when the current task
 * is cancelled (``task.cancel()`` settles the task; the coroutine body then
 * observes it at this await point).
 */
function _cancellable(_owner, promise) {
  const task = _current_task.get();
  if (task === null || task.done()) return promise;
  return new Promise((res, rej) => {
    let settled = false;
    const on_cancel = (err) => {
      if (settled) return;
      settled = true;
      rej(err);
    };
    task._add_cancel_listener(on_cancel);
    promise.then(
      (v) => {
        task._remove_cancel_listener(on_cancel);
        if (settled) return;
        settled = true;
        res(v);
      },
      (e) => {
        task._remove_cancel_listener(on_cancel);
        if (settled) return;
        settled = true;
        rej(e);
      },
    );
  });
}

/** ``asyncio.Lock`` (``async with lock``) */
export class Lock {
  constructor() {
    this._locked = false;
    this._waiters = [];
  }
  locked() {
    return this._locked;
  }
  async acquire() {
    if (!this._locked && this._waiters.length === 0) {
      this._locked = true;
      return true;
    }
    await new Promise((res) => this._waiters.push(res));
    this._locked = true;
    return true;
  }
  release() {
    if (!this._locked) throw new RuntimeError("Lock is not acquired.");
    this._locked = false;
    const next = this._waiters.shift();
    if (next) next();
  }
  async __aenter__() {
    await this.acquire();
    return null;
  }
  async __aexit__() {
    this.release();
    return false;
  }
}

/** ``asyncio.Semaphore`` */
export class Semaphore {
  constructor(value = 1) {
    if (value < 0) throw new ValueError("Semaphore initial value must be >= 0");
    this._value = value;
    this._waiters = [];
  }
  locked() {
    return this._value === 0;
  }
  async acquire() {
    if (this._value > 0 && this._waiters.length === 0) {
      this._value -= 1;
      return true;
    }
    await new Promise((res) => this._waiters.push(res));
    this._value -= 1;
    return true;
  }
  release() {
    this._value += 1;
    const next = this._waiters.shift();
    if (next) next();
  }
  async __aenter__() {
    await this.acquire();
    return null;
  }
  async __aexit__() {
    this.release();
    return false;
  }
}

/** ``asyncio.Queue`` */
export class Queue {
  constructor(maxsize = 0) {
    this._maxsize = maxsize;
    this._items = [];
    this._getters = [];
    this._putters = [];
    this._unfinished = 0;
    this._join_waiters = [];
  }
  qsize() {
    return this._items.length;
  }
  get maxsize() {
    return this._maxsize;
  }
  empty() {
    return this._items.length === 0;
  }
  full() {
    return this._maxsize > 0 && this._items.length >= this._maxsize;
  }
  put_nowait(item) {
    if (this.full()) throw new QueueFull();
    this._items.push(item);
    this._unfinished += 1;
    // Wake every getter: a cancelled one is stale and the others re-check.
    const gs = this._getters;
    this._getters = [];
    for (const g of gs) g();
  }
  async put(item) {
    while (this.full()) await _cancellable(this, new Promise((res) => this._putters.push(res)));
    this.put_nowait(item);
  }
  get_nowait() {
    if (this.empty()) throw new QueueEmpty();
    const item = this._items.shift();
    const ps = this._putters;
    this._putters = [];
    for (const p of ps) p();
    return item;
  }
  async get() {
    while (this.empty()) await _cancellable(this, new Promise((res) => this._getters.push(res)));
    return this.get_nowait();
  }
  task_done() {
    if (this._unfinished <= 0) throw new ValueError("task_done() called too many times");
    this._unfinished -= 1;
    if (this._unfinished === 0) {
      const ws = this._join_waiters;
      this._join_waiters = [];
      for (const w of ws) w();
    }
  }
  join() {
    if (this._unfinished === 0) return Promise.resolve();
    return new Promise((res) => this._join_waiters.push(res));
  }
}

// --------------------------------------------------------------------------
// Streams (``asyncio.open_connection`` / ``asyncio.start_server`` / unix)
// --------------------------------------------------------------------------

/** ``asyncio.IncompleteReadError`` */
export class IncompleteReadError extends Error {
  constructor(partial, expected) {
    super(`${partial.length} bytes read on a total of ${expected === null ? "undefined" : expected} expected bytes`);
    this.name = "IncompleteReadError";
    this.partial = partial;
    this.expected = expected;
  }
}
/** ``asyncio.LimitOverrunError`` */
export class LimitOverrunError extends Error {
  constructor(message, consumed) {
    super(message);
    this.name = "LimitOverrunError";
    this.consumed = consumed;
  }
}

/**
 * ``asyncio.StreamReader``: a byte buffer fed by a transport
 * (``feed_data`` / ``feed_eof``) and drained by ``readexactly`` / ``read`` /
 * ``readuntil`` / ``readline``. All reads are cancellation-aware.
 */
export class StreamReader {
  constructor(limit = 2 ** 16) {
    this._limit = limit;
    /** @type {Buffer[]} */
    this._chunks = [];
    this._size = 0;
    this._eof = false;
    this._exc = null;
    /** @type {Array<() => void>} */
    this._waiters = [];
    this._transport = null;
    this._paused = false;
  }
  set_transport(t) {
    this._transport = t;
  }
  set_exception(exc) {
    this._exc = exc;
    this._wake();
  }
  exception() {
    return this._exc;
  }
  at_eof() {
    return this._eof && this._size === 0;
  }
  feed_eof() {
    this._eof = true;
    this._wake();
  }
  feed_data(data) {
    if (!data || data.length === 0) return;
    const b = Buffer.isBuffer(data) ? data : Buffer.from(data);
    this._chunks.push(b);
    this._size += b.length;
    this._wake();
    // Flow control: pause the transport when buffering too much.
    if (this._transport && !this._paused && this._size > 2 * this._limit && typeof this._transport.pause_reading === "function") {
      try {
        this._transport.pause_reading();
        this._paused = true;
      } catch {
        this._paused = false;
      }
    }
  }
  _wake() {
    const ws = this._waiters;
    this._waiters = [];
    for (const w of ws) w();
  }
  _maybe_resume() {
    if (this._paused && this._transport && this._size <= this._limit && typeof this._transport.resume_reading === "function") {
      this._paused = false;
      try {
        this._transport.resume_reading();
      } catch {
        /* closed */
      }
    }
  }
  async _wait_for_data() {
    if (this._exc) throw this._exc;
    await _cancellable(this, new Promise((res) => this._waiters.push(res)));
    if (this._exc) throw this._exc;
  }
  _take(n) {
    // n <= this._size
    if (n === this._size && this._chunks.length === 1) {
      const out = this._chunks[0];
      this._chunks = [];
      this._size = 0;
      this._maybe_resume();
      return out;
    }
    const out = Buffer.allocUnsafe(n);
    let off = 0;
    while (off < n) {
      const c = this._chunks[0];
      const take = Math.min(c.length, n - off);
      c.copy(out, off, 0, take);
      off += take;
      if (take === c.length) this._chunks.shift();
      else this._chunks[0] = c.subarray(take);
    }
    this._size -= n;
    this._maybe_resume();
    return out;
  }
  /** ``await reader.readexactly(n)`` -- raises ``IncompleteReadError`` on EOF. */
  async readexactly(n) {
    if (n < 0) throw new ValueError("readexactly size can not be less than zero");
    if (this._exc) throw this._exc;
    if (n === 0) return Buffer.alloc(0);
    while (this._size < n) {
      if (this._eof) {
        const partial = this._take(this._size);
        throw new IncompleteReadError(partial, n);
      }
      await this._wait_for_data();
    }
    return this._take(n);
  }
  /** ``await reader.read(n=-1)`` -- up to *n* bytes (``b""`` at EOF). */
  async read(n = -1) {
    if (this._exc) throw this._exc;
    if (n === 0) return Buffer.alloc(0);
    if (n < 0) {
      const parts = [];
      for (;;) {
        const block = await this.read(this._limit);
        if (block.length === 0) break;
        parts.push(block);
      }
      return Buffer.concat(parts);
    }
    while (this._size === 0 && !this._eof) await this._wait_for_data();
    if (this._size === 0) return Buffer.alloc(0);
    return this._take(Math.min(n, this._size));
  }
  /** ``await reader.readuntil(separator=b"\\n")`` */
  async readuntil(separator = Buffer.from("\n")) {
    const sep = Buffer.isBuffer(separator) ? separator : Buffer.from(separator);
    if (sep.length === 0) throw new ValueError("Separator should be at least one-byte string");
    if (this._exc) throw this._exc;
    let search_from = 0;
    for (;;) {
      const buf = Buffer.concat(this._chunks, this._size);
      const idx = buf.indexOf(sep, Math.max(0, search_from));
      if (idx !== -1) {
        if (idx > this._limit) throw new LimitOverrunError("Separator is found, but chunk is longer than limit", idx);
        return this._take(idx + sep.length);
      }
      if (this._size > this._limit) throw new LimitOverrunError("Separator is not found, and chunk exceed the limit", this._size);
      if (this._eof) {
        const partial = this._take(this._size);
        throw new IncompleteReadError(partial, null);
      }
      search_from = Math.max(0, this._size - sep.length + 1);
      await this._wait_for_data();
    }
  }
  /** ``await reader.readline()`` */
  async readline() {
    try {
      return await this.readuntil(Buffer.from("\n"));
    } catch (e) {
      if (e instanceof IncompleteReadError) return e.partial;
      if (e instanceof LimitOverrunError) {
        this._take(Math.min(e.consumed, this._size));
        throw new ValueError(e.message);
      }
      throw e;
    }
  }
  [Symbol.asyncIterator]() {
    return {
      next: async () => {
        const line = await this.readline();
        return line.length === 0 ? { done: true, value: undefined } : { done: false, value: line };
      },
    };
  }
}

/**
 * ``asyncio.Transport`` over a ``net.Socket`` / ``tls.TLSSocket``.
 */
export class _SocketTransport {
  constructor(socket, protocol = null) {
    this._sock = socket;
    this._protocol = protocol;
    this._closing = false;
    this._extra = {};
  }
  get_extra_info(name, dflt = null) {
    const s = this._sock;
    switch (name) {
      case "socket":
        return s;
      case "peername":
        return s.remoteAddress !== undefined ? [s.remoteAddress, s.remotePort] : dflt;
      case "sockname": {
        const a = typeof s.address === "function" ? s.address() : null;
        return a && a.address !== undefined ? [a.address, a.port] : dflt;
      }
      case "ssl_object":
      case "sslcontext":
        return typeof s.getPeerCertificate === "function" ? s : dflt;
      case "peercert":
        return typeof s.getPeerCertificate === "function" ? s.getPeerCertificate() : dflt;
      case "cipher":
        return typeof s.getCipher === "function" ? s.getCipher() : dflt;
      default:
        return name in this._extra ? this._extra[name] : dflt;
    }
  }
  is_closing() {
    return this._closing || this._sock.destroyed || this._sock.writableEnded;
  }
  write(data) {
    if (this.is_closing()) return;
    this._sock.write(Buffer.isBuffer(data) ? data : Buffer.from(data));
  }
  writelines(list) {
    for (const d of list) this.write(d);
  }
  can_write_eof() {
    return true;
  }
  write_eof() {
    this._sock.end();
  }
  pause_reading() {
    this._sock.pause();
  }
  resume_reading() {
    this._sock.resume();
  }
  get_write_buffer_size() {
    return this._sock.writableLength;
  }
  /** ``transport.set_write_buffer_limits(high=..., low=...)`` -- recorded; Node sizes its own kernel buffer. */
  set_write_buffer_limits(opts = {}) {
    const high = typeof opts === "number" ? opts : opts.high;
    const low = typeof opts === "number" ? undefined : opts.low;
    this._write_limits = { high: high ?? 64 * 1024, low: low ?? (high ?? 64 * 1024) >> 2 };
  }
  get_write_buffer_limits() {
    const l = this._write_limits ?? { high: 64 * 1024, low: 16 * 1024 };
    return [l.low, l.high];
  }
  close() {
    if (this._closing) return;
    this._closing = true;
    this._sock.end();
    // A peer that never acknowledges keeps a half-closed socket around; let
    // the loop reclaim it eventually.
    const t = setTimeout(() => this._sock.destroy(), 2000);
    if (typeof t.unref === "function") t.unref();
    this._sock.once("close", () => clearTimeout(t));
  }
  abort() {
    this._closing = true;
    this._sock.destroy();
  }
}

/** ``asyncio.StreamWriter`` */
export class StreamWriter {
  constructor(transport, reader = null) {
    this._transport = transport;
    this._reader = reader;
    this._sock = transport._sock;
    this._closed = new Future();
    this._sock.once("close", () => {
      if (!this._closed.done()) this._closed.set_result(null);
    });
  }
  get transport() {
    return this._transport;
  }
  get_extra_info(name, dflt = null) {
    return this._transport.get_extra_info(name, dflt);
  }
  write(data) {
    this._transport.write(data);
  }
  writelines(list) {
    this._transport.writelines(list);
  }
  write_eof() {
    this._transport.write_eof();
  }
  can_write_eof() {
    return true;
  }
  is_closing() {
    return this._transport.is_closing();
  }
  close() {
    this._transport.close();
  }
  /** ``await writer.drain()`` -- waits for the kernel buffer to accept more. */
  async drain() {
    if (this._reader && this._reader._exc) throw this._reader._exc;
    const s = this._sock;
    if (s.destroyed) {
      const err = new Error("Connection lost");
      err.code = "ECONNRESET";
      throw err;
    }
    if (!s.writableNeedDrain) return;
    await new Promise((res, rej) => {
      const on_drain = () => {
        cleanup();
        res();
      };
      const on_close = () => {
        cleanup();
        const err = new Error("Connection lost");
        err.code = "ECONNRESET";
        rej(err);
      };
      const on_error = (e) => {
        cleanup();
        rej(e);
      };
      const cleanup = () => {
        s.off("drain", on_drain);
        s.off("close", on_close);
        s.off("error", on_error);
      };
      s.on("drain", on_drain);
      s.on("close", on_close);
      s.on("error", on_error);
    });
  }
  /** ``await writer.wait_closed()`` */
  wait_closed() {
    return this._closed._promise;
  }
}

/** Wrap a connected socket into ``[reader, writer]``. */
export function _wrap_stream_socket(sock, limit = 2 ** 16) {
  const reader = new StreamReader(limit);
  const transport = new _SocketTransport(sock);
  reader.set_transport(transport);
  const writer = new StreamWriter(transport, reader);
  sock.on("data", (chunk) => reader.feed_data(chunk));
  sock.on("end", () => reader.feed_eof());
  sock.on("error", (err) => {
    reader.set_exception(err);
  });
  sock.on("close", () => {
    if (!reader._eof) reader.feed_eof();
  });
  return [reader, writer];
}

function _ssl_options(ssl, server_hostname = null) {
  if (!ssl) return null;
  let opts;
  if (typeof ssl.to_node_options === "function") opts = { ...ssl.to_node_options() };
  else if (ssl === true) opts = {};
  else opts = { ...ssl };
  if (server_hostname) opts.servername = server_hostname;
  return opts;
}

/**
 * ``asyncio.open_connection(host, port, ssl=..., server_hostname=..., limit=...)``
 * -> ``[reader, writer]``.
 */
export async function open_connection(host = null, port = null, opts = {}) {
  const { ssl = null, server_hostname = null, limit = 2 ** 16, local_addr = null, sock = null } = opts;
  const net = await import("node:net");
  const socket = await new Promise((res, rej) => {
    let s;
    const on_err = (e) => rej(e);
    if (sock) {
      s = sock;
      res(s);
      return;
    }
    const ssl_opts = _ssl_options(ssl, server_hostname ?? host);
    if (ssl_opts) {
      import("node:tls").then((tls) => {
        s = tls.connect({ host, port, ...ssl_opts, ...(local_addr ? { localAddress: local_addr[0], localPort: local_addr[1] } : {}) }, () => {
          s.off("error", on_err);
          res(s);
        });
        s.once("error", on_err);
      }, rej);
    } else {
      s = net.createConnection({ host, port, ...(local_addr ? { localAddress: local_addr[0], localPort: local_addr[1] } : {}) }, () => {
        s.off("error", on_err);
        res(s);
      });
      s.once("error", on_err);
    }
  });
  socket.setNoDelay(true);
  return _wrap_stream_socket(socket, limit);
}

/** ``asyncio.open_unix_connection(path, ssl=...)`` -> ``[reader, writer]``. */
export async function open_unix_connection(path, opts = {}) {
  const { ssl = null, server_hostname = null, limit = 2 ** 16 } = opts;
  const net = await import("node:net");
  const socket = await new Promise((res, rej) => {
    const s = net.createConnection({ path }, () => {
      s.off("error", on_err);
      res(s);
    });
    const on_err = (e) => rej(e);
    s.once("error", on_err);
  });
  if (ssl) {
    const tls = await import("node:tls");
    const ts = await new Promise((res, rej) => {
      const t = tls.connect({ socket, ..._ssl_options(ssl, server_hostname) }, () => {
        t.off("error", on_err);
        res(t);
      });
      const on_err = (e) => rej(e);
      t.once("error", on_err);
    });
    return _wrap_stream_socket(ts, limit);
  }
  return _wrap_stream_socket(socket, limit);
}

/** ``asyncio.Server`` over a ``net.Server`` / ``tls.Server``. */
export class Server {
  constructor(server) {
    this._server = server;
    this._closed = new Future();
    this._serving = true;
    /** @type {Set<import("node:net").Socket>} */
    this._conns = new Set();
    server.on("connection", (s) => {
      this._conns.add(s);
      s.once("close", () => this._conns.delete(s));
    });
    server.on("secureConnection", (s) => {
      this._conns.add(s);
      s.once("close", () => this._conns.delete(s));
    });
    server.once("close", () => {
      this._serving = false;
      if (!this._closed.done()) this._closed.set_result(null);
    });
  }
  /** ``server.sockets`` -- ``[{getsockname(): [host, port]}]`` */
  get sockets() {
    const a = this._server.address();
    if (a === null) return [];
    const addr = typeof a === "string" ? a : [a.address, a.port];
    return [
      {
        getsockname: () => addr,
        family: typeof a === "string" ? 1 : a.family === "IPv6" ? 10 : 2,
      },
    ];
  }
  is_serving() {
    return this._serving && this._server.listening;
  }
  /** ``server.close()`` -- stop accepting; existing connections keep going. */
  close() {
    if (!this._serving) return;
    this._serving = false;
    try {
      this._server.close();
    } catch {
      /* already closed */
    }
  }
  /** Python 3.13 ``server.close_clients()`` */
  close_clients() {
    for (const s of [...this._conns]) s.end();
  }
  abort_clients() {
    for (const s of [...this._conns]) s.destroy();
  }
  /** ``await server.wait_closed()`` */
  wait_closed() {
    return this._closed._promise;
  }
  async start_serving() {}
  async serve_forever() {
    await this._closed._promise;
  }
  async __aenter__() {
    return this;
  }
  async __aexit__() {
    this.close();
    await this.wait_closed();
    return false;
  }
}

function _bind_server(server, listen_opts) {
  return new Promise((res, rej) => {
    const on_err = (e) => rej(e);
    server.once("error", on_err);
    server.listen(listen_opts, () => {
      server.off("error", on_err);
      // Surface late errors (e.g. EADDRINUSE on retry) without crashing.
      server.on("error", () => {});
      res(server);
    });
  });
}

/**
 * ``asyncio.start_server(client_connected_cb, host, port, ssl=..., backlog=..., reuse_address=...)``
 * -> ``Server``. The callback receives ``(reader, writer)`` and runs as a task.
 */
export async function start_server(client_connected_cb, host = null, port = null, opts = {}) {
  const { ssl = null, limit = 2 ** 16, backlog = 100, reuse_port = false, start_serving = true } = opts;
  void start_serving;
  const net = await import("node:net");
  let server;
  const on_conn = (sock) => {
    sock.setNoDelay(true);
    const [reader, writer] = _wrap_stream_socket(sock, limit);
    create_task(() => Promise.resolve(client_connected_cb(reader, writer)));
  };
  const ssl_opts = _ssl_options(ssl);
  if (ssl_opts) {
    const tls = await import("node:tls");
    server = tls.createServer(ssl_opts, on_conn);
  } else {
    server = net.createServer(on_conn);
  }
  const listen_opts = { port: port ?? 0, backlog };
  if (host !== null && host !== undefined && host !== "") listen_opts.host = host;
  if (reuse_port) listen_opts.reusePort = true;
  await _bind_server(server, listen_opts);
  return new Server(server);
}

/** ``asyncio.start_unix_server(cb, path, ssl=...)`` -> ``Server``. */
export async function start_unix_server(client_connected_cb, path, opts = {}) {
  const { ssl = null, limit = 2 ** 16, backlog = 100 } = opts;
  const net = await import("node:net");
  const on_conn = (sock) => {
    const [reader, writer] = _wrap_stream_socket(sock, limit);
    create_task(() => Promise.resolve(client_connected_cb(reader, writer)));
  };
  let server;
  const ssl_opts = _ssl_options(ssl);
  if (ssl_opts) {
    const tls = await import("node:tls");
    server = tls.createServer(ssl_opts, on_conn);
  } else {
    server = net.createServer(on_conn);
  }
  await _bind_server(server, { path, backlog });
  return new Server(server);
}

// --------------------------------------------------------------------------
// Datagram endpoints (``loop.create_datagram_endpoint``)
// --------------------------------------------------------------------------

/** ``asyncio.BaseProtocol`` */
export class BaseProtocol {
  connection_made(_transport) {}
  connection_lost(_exc) {}
  pause_writing() {}
  resume_writing() {}
}
/** ``asyncio.Protocol`` (streaming) */
export class Protocol extends BaseProtocol {
  data_received(_data) {}
  eof_received() {
    return false;
  }
}
/** ``asyncio.DatagramProtocol`` */
export class DatagramProtocol extends BaseProtocol {
  datagram_received(_data, _addr) {}
  error_received(_exc) {}
}

/** ``asyncio.DatagramTransport`` over a ``dgram.Socket``. */
export class DatagramTransport {
  constructor(socket, protocol, remote_addr = null) {
    this._sock = socket;
    this._protocol = protocol;
    this._remote = remote_addr;
    this._closing = false;
    this._extra = {};
  }
  get_extra_info(name, dflt = null) {
    switch (name) {
      case "socket":
        return this._sock;
      case "sockname": {
        try {
          const a = this._sock.address();
          return [a.address, a.port];
        } catch {
          return dflt;
        }
      }
      case "peername":
        return this._remote ?? dflt;
      default:
        return name in this._extra ? this._extra[name] : dflt;
    }
  }
  is_closing() {
    return this._closing;
  }
  /** ``transport.sendto(data, addr=None)`` */
  sendto(data, addr = null) {
    if (this._closing) return;
    const target = addr ?? this._remote;
    if (!target) throw new ValueError("sendto() requires an address when the endpoint is not connected");
    const buf = Buffer.isBuffer(data) ? data : Buffer.from(data);
    this._sock.send(buf, 0, buf.length, target[1], target[0], (err) => {
      if (err && this._protocol && typeof this._protocol.error_received === "function") {
        try {
          this._protocol.error_received(err);
        } catch {
          /* protocol error handlers must not crash the loop */
        }
      }
    });
  }
  abort() {
    this.close();
  }
  close() {
    if (this._closing) return;
    this._closing = true;
    try {
      this._sock.close();
    } catch {
      /* already closed */
    }
  }
}

/**
 * ``loop.create_datagram_endpoint(protocol_factory, local_addr=(host, port), remote_addr=..., reuse_port=..., allow_broadcast=...)``
 * -> ``[transport, protocol]``.
 */
export async function create_datagram_endpoint(protocol_factory, opts = {}) {
  const { local_addr = null, remote_addr = null, reuse_port = false, allow_broadcast = false, family = 0 } = opts;
  const dgram = await import("node:dgram");
  const net = await import("node:net");
  let type = "udp4";
  const probe = local_addr?.[0] || remote_addr?.[0] || null;
  if (family === 10 || (probe && net.isIPv6(probe))) type = "udp6";
  const socket = dgram.createSocket({ type, reusePort: !!reuse_port, reuseAddr: !!reuse_port });
  const protocol = protocol_factory();
  const transport = new DatagramTransport(socket, protocol, remote_addr);
  await new Promise((res, rej) => {
    const on_err = (e) => rej(e);
    socket.once("error", on_err);
    const bind_cb = () => {
      socket.off("error", on_err);
      res();
    };
    if (local_addr) {
      const [host, port] = local_addr;
      if (host && host !== "") socket.bind(port ?? 0, host, bind_cb);
      else socket.bind(port ?? 0, bind_cb);
    } else socket.bind(0, bind_cb);
  });
  if (allow_broadcast) {
    try {
      socket.setBroadcast(true);
    } catch {
      /* not supported */
    }
  }
  socket.on("message", (msg, rinfo) => {
    try {
      protocol.datagram_received(msg, [rinfo.address, rinfo.port]);
    } catch (e) {
      // CPython logs "Exception in callback" and keeps the loop going.
      queueMicrotask(() => {
        void e;
      });
    }
  });
  socket.on("error", (e) => {
    try {
      protocol.error_received(e);
    } catch {
      /* ignore */
    }
  });
  socket.once("close", () => {
    try {
      protocol.connection_lost(null);
    } catch {
      /* ignore */
    }
  });
  protocol.connection_made(transport);
  return [transport, protocol];
}
