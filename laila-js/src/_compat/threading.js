/**
 * Python ``threading`` primitives for a single-threaded runtime.
 *
 * "Threads" are execution contexts (see contextvars.js). Blocking waits
 * (``lock.acquire()``, ``event.wait()``, ``thread.join()``) spin the event loop
 * through the native pump so other contexts progress underneath the caller,
 * which is exactly what a blocked Python thread observes. Every primitive also
 * offers an ``*_async`` form for coroutine code.
 *
 * Deadlock guard (rule L1): a blocking *wait* performed while the current
 * sync chain holds any lock cannot be satisfied if the completing callback
 * needs that lock (it can never run "underneath" the holder), so
 * ``blocking_wait`` raises ``RuntimeError`` instead of hanging. Contended
 * ``acquire()`` itself is allowed to pump (nested-lock patterns), mirroring
 * Python's blocking behaviour.
 */
import { pump_until, pump_available, pump_unavailable_reason, in_microtask, hop } from "./pump.js";
import { current_context, get_ident, run_in_new_context, Context } from "./contextvars.js";
import { RuntimeError, ValueError, TimeoutError as PyTimeoutError } from "./errors.js";

export { get_ident };

// --------------------------------------------------------------------------
// Pump wrapper with the lock-depth guard
// --------------------------------------------------------------------------

// Count of locks held by each nesting level of synchronous execution. Index 0
// is the outermost sync chain; a nested pump pushes a fresh level because the
// callbacks it runs are *different* Python threads.
const _held_levels = [0];

function _held_here() {
  return _held_levels[_held_levels.length - 1];
}
function _lock_taken() {
  _held_levels[_held_levels.length - 1] += 1;
}
function _lock_released() {
  const i = _held_levels.length - 1;
  if (_held_levels[i] > 0) _held_levels[i] -= 1;
}

/**
 * Error raised when synchronous blocking is impossible in this context.
 * Mapped to ``LoopBlockingWaitError`` by the futures layer.
 */
export class BlockingNotPossibleError extends RuntimeError {}

/**
 * Block until ``pred()`` is truthy or ``timeout`` seconds pass.
 * @param {() => boolean} pred
 * @param {number|null} [timeout] seconds; ``null`` blocks indefinitely
 * @param {{allow_locked?: boolean}} [opts] skip the L1 guard (used by lock acquire)
 * @returns {boolean} false on timeout
 */
export function blocking_wait(pred, timeout = null, opts = {}) {
  if (pred()) return true;
  if (!opts.allow_locked && _held_here() > 0) {
    throw new BlockingNotPossibleError(
      "blocking wait while holding a lock: the callback that would complete it can never run " +
        "underneath this frame (release the lock before waiting, as Future.result does)",
    );
  }
  const locked = _held_here() > 0;
  _held_levels.push(0);
  try {
    return pump_until(pred, timeout, locked);
  } catch (err) {
    if (err && (err.code === "ERR_LAILA_PUMP_UNAVAILABLE" || err.code === "ERR_LAILA_PUMP_IN_MICROTASK" || err.code === "ERR_LAILA_PUMP_DEPTH")) {
      const e = new BlockingNotPossibleError(err.message);
      e.code = err.code;
      throw e;
    }
    throw err;
  } finally {
    _held_levels.pop();
  }
}

/** True if a synchronous blocking wait could succeed right now. */
export function can_block() {
  return pump_available() && !in_microtask();
}

export { pump_available, pump_unavailable_reason, in_microtask };

// --------------------------------------------------------------------------
// Waiter registry shared by the primitives (async waiters)
// --------------------------------------------------------------------------

class _Waiters {
  constructor() {
    this._list = [];
  }
  /**
   * Returns a promise resolving true when notified, false on timeout.
   * ``unref`` makes the timeout timer not keep the process alive (daemon
   * threads polling a condition in Python do not keep the interpreter alive).
   */
  wait(timeout, unref = false) {
    return new Promise((resolve) => {
      const w = { resolve, timer: null };
      if (timeout !== null && timeout !== undefined) {
        w.timer = setTimeout(() => {
          this._remove(w);
          resolve(false);
        }, Math.max(0, timeout * 1000));
        if (unref && typeof w.timer.unref === "function") w.timer.unref();
      }
      this._list.push(w);
    });
  }
  _remove(w) {
    const i = this._list.indexOf(w);
    if (i >= 0) this._list.splice(i, 1);
  }
  notify(n = 1) {
    for (let k = 0; k < n && this._list.length; k++) {
      const w = this._list.shift();
      if (w.timer) clearTimeout(w.timer);
      w.resolve(true);
    }
  }
  notify_all() {
    this.notify(this._list.length);
  }
  get size() {
    return this._list.length;
  }
}

// --------------------------------------------------------------------------
// Locks
// --------------------------------------------------------------------------

/** ``threading.RLock`` */
export class RLock {
  constructor() {
    this._owner = null;
    this._count = 0;
    this._waiters = new _Waiters();
  }
  /**
   * ``acquire(blocking=True, timeout=-1)``
   * @param {{blocking?: boolean, timeout?: number}} [opts]
   */
  acquire(opts = {}) {
    const blocking = opts.blocking !== false;
    const timeout = opts.timeout === undefined || opts.timeout === null ? -1 : opts.timeout;
    const me = get_ident();
    if (this._count === 0) {
      this._owner = me;
      this._count = 1;
      _lock_taken();
      return true;
    }
    if (this._owner === me) {
      this._count += 1;
      return true;
    }
    if (!blocking) return false;
    const ok = blocking_wait(() => this._count === 0, timeout < 0 ? null : timeout, { allow_locked: true });
    if (!ok) return false;
    this._owner = me;
    this._count = 1;
    _lock_taken();
    return true;
  }
  /** Coroutine-friendly acquire. */
  async acquire_async(timeout = null) {
    const me = get_ident();
    for (;;) {
      if (this._count === 0) {
        this._owner = me;
        this._count = 1;
        _lock_taken();
        return true;
      }
      if (this._owner === me) {
        this._count += 1;
        return true;
      }
      const ok = await this._waiters.wait(timeout);
      if (!ok) return false;
    }
  }
  release() {
    if (this._count === 0 || this._owner !== get_ident()) throw new RuntimeError("cannot release un-acquired lock");
    this._count -= 1;
    if (this._count === 0) {
      this._owner = null;
      _lock_released();
      this._waiters.notify(1);
    }
  }
  locked() {
    return this._count > 0;
  }
  /** Python ``RLock._is_owned()`` */
  _is_owned() {
    return this._count > 0 && this._owner === get_ident();
  }
  __enter__() {
    this.acquire();
    return this;
  }
  __exit__() {
    this.release();
    return false;
  }
  [Symbol.dispose]() {
    this.release();
  }
  __repr__() {
    return `<${this.locked() ? "locked" : "unlocked"} RLock owner=${this._owner} count=${this._count}>`;
  }
}

/** ``threading.Lock`` (non-reentrant). */
export class Lock {
  constructor() {
    this._held = false;
    this._owner = null;
    this._waiters = new _Waiters();
  }
  acquire(opts = {}) {
    const blocking = opts.blocking !== false;
    const timeout = opts.timeout === undefined || opts.timeout === null ? -1 : opts.timeout;
    if (!this._held) {
      this._held = true;
      this._owner = get_ident();
      this._owner_ctx = current_context().ident;
      _lock_taken();
      return true;
    }
    if (!blocking) return false;
    if (this._owner_ctx === current_context().ident && timeout < 0) {
      // Python would deadlock forever here (same thread re-acquiring a
      // non-reentrant lock); fail loudly instead.
      throw new RuntimeError("deadlock: Lock is already held by the current context");
    }
    const ok = blocking_wait(() => !this._held, timeout < 0 ? null : timeout, { allow_locked: true });
    if (!ok) return false;
    this._held = true;
    this._owner = get_ident();
    this._owner_ctx = current_context().ident;
    _lock_taken();
    return true;
  }
  async acquire_async(timeout = null) {
    for (;;) {
      if (!this._held) {
        this._held = true;
        this._owner = get_ident();
        this._owner_ctx = current_context().ident;
        _lock_taken();
        return true;
      }
      const ok = await this._waiters.wait(timeout);
      if (!ok) return false;
    }
  }
  release() {
    if (!this._held) throw new RuntimeError("release unlocked lock");
    this._held = false;
    this._owner = null;
    this._owner_ctx = null;
    _lock_released();
    this._waiters.notify(1);
  }
  locked() {
    return this._held;
  }
  __enter__() {
    this.acquire();
    return this;
  }
  __exit__() {
    this.release();
    return false;
  }
  [Symbol.dispose]() {
    this.release();
  }
}

/** Python ``with lock:`` -> ``with_lock(lock, () => ...)`` */
export function with_lock(lock, fn) {
  lock.acquire();
  try {
    return fn();
  } finally {
    lock.release();
  }
}

/** Async variant: ``async with``-like usage over ``acquire_async``. */
export async function with_lock_async(lock, fn) {
  await lock.acquire_async();
  try {
    return await fn();
  } finally {
    lock.release();
  }
}

// --------------------------------------------------------------------------
// Event / Condition / Semaphore
// --------------------------------------------------------------------------

/** ``threading.Event`` */
export class Event {
  constructor() {
    this._flag = false;
    this._waiters = new _Waiters();
  }
  is_set() {
    return this._flag;
  }
  isSet() {
    return this._flag;
  }
  set() {
    this._flag = true;
    this._waiters.notify_all();
  }
  clear() {
    this._flag = false;
  }
  /** Blocking ``wait(timeout=None)`` -> bool */
  wait(timeout = null) {
    if (this._flag) return true;
    return blocking_wait(() => this._flag, timeout);
  }
  /** ``asyncio.Event.wait``-like */
  async wait_async(timeout = null) {
    if (this._flag) return true;
    return this._waiters.wait(timeout);
  }
}

/** ``threading.Condition`` */
export class Condition {
  constructor(lock = null) {
    this._lock = lock ?? new RLock();
    this._waiters = new _Waiters();
    this._pending = [];
  }
  acquire(opts) {
    return this._lock.acquire(opts);
  }
  release() {
    return this._lock.release();
  }
  __enter__() {
    this._lock.acquire();
    return this;
  }
  __exit__() {
    this._lock.release();
    return false;
  }
  _release_save() {
    const lock = this._lock;
    if (lock instanceof RLock) {
      const state = { owner: lock._owner, count: lock._count };
      lock._count = 0;
      lock._owner = null;
      _lock_released();
      lock._waiters.notify(1);
      return state;
    }
    lock.release();
    return null;
  }
  _acquire_restore(state) {
    const lock = this._lock;
    if (lock instanceof RLock) {
      blocking_wait(() => lock._count === 0, null, { allow_locked: true });
      lock._owner = state.owner;
      lock._count = state.count;
      _lock_taken();
      return;
    }
    lock.acquire();
  }
  /** Blocking ``wait(timeout=None)`` -> bool (false on timeout). */
  wait(timeout = null) {
    if (!this._lock._is_owned?.() && !(this._lock instanceof Lock && this._lock.locked()))
      throw new RuntimeError("cannot wait on un-acquired lock");
    let notified = false;
    const me = { resolve: () => (notified = true) };
    this._pending.push(me);
    const state = this._release_save();
    let ok;
    try {
      ok = blocking_wait(() => notified, timeout);
    } finally {
      const i = this._pending.indexOf(me);
      if (i >= 0) this._pending.splice(i, 1);
      this._acquire_restore(state);
    }
    return ok;
  }
  async wait_async(timeout = null) {
    let notified = false;
    const me = { resolve: () => (notified = true) };
    this._pending.push(me);
    const state = this._release_save();
    try {
      const deadline = timeout === null ? null : performance.now() + timeout * 1000;
      while (!notified) {
        const remaining = deadline === null ? null : Math.max(0, (deadline - performance.now()) / 1000);
        if (remaining !== null && remaining <= 0) break;
        const got = await this._waiters.wait(remaining);
        if (!got) break;
      }
    } finally {
      const i = this._pending.indexOf(me);
      if (i >= 0) this._pending.splice(i, 1);
      if (this._lock instanceof RLock) {
        await _spin_async(() => this._lock._count === 0);
        this._lock._owner = state.owner;
        this._lock._count = state.count;
        _lock_taken();
      } else await this._lock.acquire_async();
    }
    return notified;
  }
  /**
   * Await a ``notify`` *without* holding the lock.
   *
   * For coroutine-style "threads" (dispatchers) that must not hold a lock
   * across an ``await`` -- a held lock would trip the L1 deadlock guard for
   * every other sync chain on the same pump level. In this single-threaded
   * runtime the predicate check and the waiter registration happen in one
   * synchronous step, so the lost-wakeup race the lock guards against cannot
   * occur. Spurious wakeups are possible (as with any ``Condition``).
   * @param {number|null} [timeout] seconds; ``null`` waits for a notify
   * @param {{unref?: boolean}} [opts] ``unref`` keeps the timer from holding the process open
   * @returns {Promise<boolean>} false on timeout
   */
  wait_unlocked_async(timeout = null, opts = {}) {
    return this._waiters.wait(timeout, !!opts.unref);
  }
  /** ``wait_for(predicate, timeout=None)`` */
  wait_for(predicate, timeout = null) {
    const deadline = timeout === null ? null : performance.now() + timeout * 1000;
    let result = predicate();
    while (!result) {
      const remaining = deadline === null ? null : (deadline - performance.now()) / 1000;
      if (remaining !== null && remaining <= 0) break;
      this.wait(remaining);
      result = predicate();
    }
    return result;
  }
  notify(n = 1) {
    for (let k = 0; k < n && this._pending.length; k++) this._pending.shift().resolve();
    this._waiters.notify(n);
  }
  notify_all() {
    this.notify(this._pending.length + this._waiters.size);
  }
  notifyAll() {
    this.notify_all();
  }
}
async function _spin_async(pred) {
  while (!pred()) await new Promise((r) => hop(r));
}

/** ``threading.Semaphore`` */
export class Semaphore {
  constructor(value = 1) {
    if (value < 0) throw new ValueError("semaphore initial value must be >= 0");
    this._value = value;
    this._waiters = new _Waiters();
  }
  acquire(opts = {}) {
    const blocking = opts.blocking !== false;
    const timeout = opts.timeout === undefined || opts.timeout === null ? null : opts.timeout;
    if (this._value > 0) {
      this._value -= 1;
      return true;
    }
    if (!blocking) return false;
    const ok = blocking_wait(() => this._value > 0, timeout, { allow_locked: true });
    if (!ok) return false;
    this._value -= 1;
    return true;
  }
  async acquire_async(timeout = null) {
    for (;;) {
      if (this._value > 0) {
        this._value -= 1;
        return true;
      }
      const ok = await this._waiters.wait(timeout);
      if (!ok) return false;
    }
  }
  release(n = 1) {
    this._value += n;
    this._waiters.notify(n);
  }
  __enter__() {
    this.acquire();
    return this;
  }
  __exit__() {
    this.release();
    return false;
  }
  get value() {
    return this._value;
  }
}

/** ``threading.BoundedSemaphore`` */
export class BoundedSemaphore extends Semaphore {
  constructor(value = 1) {
    super(value);
    this._initial = value;
  }
  release(n = 1) {
    if (this._value + n > this._initial) throw new ValueError("Semaphore released too many times");
    super.release(n);
  }
}

// --------------------------------------------------------------------------
// threading.local
// --------------------------------------------------------------------------

/**
 * ``threading.local()``: attribute storage private to the current context.
 * Returns a Proxy so ``loc.x = 1`` / ``loc.x`` / ``getattr(loc, "x", None)`` work.
 */
export function local() {
  const key = Symbol("threading.local");
  const store = () => {
    // Per-*thread* storage: asyncio tasks on one thread share it, like Python.
    const ctx = current_context().thread_root;
    let m = ctx.locals.get(key);
    if (!m) {
      m = new Map();
      ctx.locals.set(key, m);
    }
    return m;
  };
  return new Proxy(Object.create(null), {
    get(_t, prop) {
      if (prop === Symbol.toStringTag) return "local";
      if (prop === "__dict__") return Object.fromEntries(store());
      return store().get(prop);
    },
    set(_t, prop, value) {
      store().set(prop, value);
      return true;
    },
    has(_t, prop) {
      return store().has(prop);
    },
    deleteProperty(_t, prop) {
      return store().delete(prop);
    },
    ownKeys() {
      return [...store().keys()].filter((k) => typeof k === "string");
    },
    getOwnPropertyDescriptor(_t, prop) {
      if (!store().has(prop)) return undefined;
      return { value: store().get(prop), writable: true, enumerable: true, configurable: true };
    },
  });
}

// --------------------------------------------------------------------------
// Thread
// --------------------------------------------------------------------------

let _thread_counter = 0;

/**
 * ``threading.Thread``: the target runs in a fresh execution context on the
 * next macrotask. ``join()`` blocks (pumps) until the target returns (or its
 * returned promise settles); ``daemon`` threads never keep the process alive
 * (every handle they create is the caller's responsibility to unref).
 */
export class Thread {
  constructor(opts = {}) {
    const { target = null, name = null, args = [], kwargs = null, daemon = false } = opts;
    this._target = target;
    this._args = args;
    this._kwargs = kwargs;
    this.name = name ?? `Thread-${++_thread_counter}`;
    this.daemon = daemon;
    this._started = false;
    this._alive = false;
    this._done = new Event();
    this._exception = null;
    this.ident = null;
  }
  /** Subclasses override ``run``. */
  run() {
    if (this._target) return this._kwargs ? this._target(...this._args, this._kwargs) : this._target(...this._args);
    return undefined;
  }
  start() {
    if (this._started) throw new RuntimeError("threads can only be started once");
    this._started = true;
    this._alive = true;
    const ctx = new Context(current_context(), this.name);
    this.ident = ctx.ident;
    hop(() => {
      ctx.run(() => {
        let result;
        try {
          result = this.run();
        } catch (err) {
          this._exception = err;
          this._finish();
          return;
        }
        if (result && typeof result.then === "function") {
          result.then(
            () => this._finish(),
            (err) => {
              this._exception = err;
              this._finish();
            },
          );
        } else this._finish();
      });
    });
    // The start hop is a referenced macrotask even for daemon threads: it
    // runs within one loop turn, so it cannot keep the process alive on its
    // own; daemon-ness is carried by the handles the body creates (unref'd
    // timers / condition waits).
  }
  _finish() {
    this._alive = false;
    this._done.set();
  }
  is_alive() {
    return this._alive;
  }
  /** Blocking ``join(timeout=None)``. */
  join(timeout = null) {
    if (!this._started) throw new RuntimeError("cannot join thread before it is started");
    if (get_ident() === this.ident) throw new RuntimeError("cannot join current thread");
    this._done.wait(timeout);
  }
  async join_async(timeout = null) {
    await this._done.wait_async(timeout);
  }
  get exception() {
    return this._exception;
  }
}

/** ``threading.current_thread()`` (name + ident of the current context). */
export function current_thread() {
  const ctx = current_context().thread_root;
  return { name: ctx.name, ident: ctx.thread_ident };
}

/** ``threading.main_thread()`` */
export function main_thread() {
  return { name: "MainThread", ident: 0 };
}

/** ``threading.Timer`` */
export class Timer extends Thread {
  constructor(interval, fn, opts = {}) {
    super({ ...opts, target: fn });
    this.interval = interval;
    this._cancelled = false;
    this._timer = null;
  }
  start() {
    this._started = true;
    this._alive = true;
    const ctx = new Context(current_context(), this.name);
    this.ident = ctx.ident;
    this._timer = setTimeout(() => {
      if (this._cancelled) return this._finish();
      ctx.run(() => {
        try {
          const r = this.run();
          if (r && typeof r.then === "function") r.then(() => this._finish(), (e) => ((this._exception = e), this._finish()));
          else this._finish();
        } catch (e) {
          this._exception = e;
          this._finish();
        }
      });
    }, this.interval * 1000);
    if (this.daemon) this._timer.unref();
  }
  cancel() {
    this._cancelled = true;
    if (this._timer) clearTimeout(this._timer);
    this._finish();
  }
}

export { run_in_new_context, current_context, PyTimeoutError as TimeoutError };
