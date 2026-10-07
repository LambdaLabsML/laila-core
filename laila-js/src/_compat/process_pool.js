/**
 * ``concurrent.futures.ProcessPoolExecutor`` over ``child_process.fork``.
 *
 * Exactly CPython's contract: the callable and its arguments are *pickled*
 * (``_codecs/pickle.js``) and shipped to a worker process, which unpickles,
 * calls, pickles the result and ships it back. A function is picklable only
 * by reference (``GLOBAL module.qualname``), so it must be a module-level
 * function tagged with its module (``fn.__module__ = import.meta.url``);
 * lambdas / closures raise ``PicklingError`` -- the same rule as Python's
 * "top-level picklable callables".
 *
 * The worker (``process_pool_worker.js``) imports the referenced modules
 * before unpickling, so the globals resolve to the real functions.
 *
 * Workers are spawned on demand up to ``max_workers`` (CPython >= 3.9
 * behaviour) and are ``unref``'d while idle so an executor that was never
 * shut down does not keep the process alive.
 */
import { fork } from "node:child_process";
import { fileURLToPath } from "node:url";
import os from "node:os";

import { ConcurrentFuture } from "./executor.js";
import { blocking_wait } from "./threading.js";
import { RuntimeError, BrokenProcessPool, PyException } from "./errors.js";
import * as pickle from "../_codecs/pickle.js";
import { PyTuple } from "./pytypes.js";
import * as E from "./errors.js";

const _WORKER = fileURLToPath(new URL("./process_pool_worker.js", import.meta.url));

class _Worker {
  constructor(pool, idx) {
    this.pool = pool;
    this.idx = idx;
    this.busy = null; // {id, fut}
    this.child = fork(_WORKER, [], {
      serialization: "advanced",
      stdio: ["ignore", "inherit", "inherit", "ipc"],
      execArgv: process.execArgv.filter((a) => !a.startsWith("--test")),
    });
    this.child.on("message", (msg) => this._on_message(msg));
    this.child.on("exit", (code, signal) => this._on_exit(code, signal));
    this.child.on("error", () => {});
    this.unref();
  }
  ref() {
    this.child.ref();
    this.child.channel?.ref?.();
  }
  unref() {
    this.child.unref();
    this.child.channel?.unref?.();
  }
  run(item) {
    this.busy = item;
    this.ref();
    this.child.send({ id: item.id, payload: item.payload, globals: item.globals });
  }
  _on_message(msg) {
    const item = this.busy;
    if (!item || msg.id !== item.id) return;
    this.busy = null;
    this.unref();
    if (msg.ok) {
      let value;
      try {
        value = pickle.loads(Buffer.from(msg.payload));
      } catch (err) {
        if (!item.fut.done()) item.fut.set_exception(err);
        this.pool._next();
        return;
      }
      if (!item.fut.done()) item.fut.set_result(value);
    } else if (!item.fut.done()) item.fut.set_exception(_rebuild_error(msg.error));
    this.pool._next();
  }
  _on_exit(code, signal) {
    this.pool._workers.delete(this);
    const item = this.busy;
    this.busy = null;
    if (item && !item.fut.done()) {
      item.fut.set_exception(
        new BrokenProcessPool(`A process in the process pool was terminated abruptly while the future was running or pending (code=${code}, signal=${signal}).`),
      );
    }
    if (!this.pool._shutdown) this.pool._next();
  }
  kill() {
    try {
      this.child.kill();
    } catch {
      /* already gone */
    }
  }
}

/** Rebuild an exception from the worker's ``{name, message, stack}`` view. */
function _rebuild_error(info) {
  const Cls = (info && info.name && E[info.name]) || null;
  let err;
  if (typeof Cls === "function" && Cls.prototype instanceof Error) err = new Cls(info.message);
  else {
    err = new PyException(info?.message ?? "worker failed");
    if (info?.name) err.name = info.name;
  }
  if (info?.stack) err.remote_stack = info.stack;
  return err;
}

/** ``concurrent.futures.ProcessPoolExecutor(max_workers=None, mp_context=None)`` */
export class ProcessPoolExecutor {
  constructor(opts = {}) {
    const { max_workers = null, mp_context = null } = opts;
    this._max_workers = max_workers ?? Math.max(1, os.cpus().length || 1);
    this._mp_context = mp_context;
    this._workers = new Set();
    this._queue = [];
    this._shutdown = false;
    this._broken = null;
    this._counter = 0;
  }

  /**
   * ``submit(fn, *args)``: pickles ``(fn, args)`` *now* (so unpicklable
   * callables fail at submission, like CPython) and dispatches to a worker.
   */
  submit(fn, ...args) {
    if (this._broken) throw new BrokenProcessPool(this._broken);
    if (this._shutdown) throw new RuntimeError("cannot schedule new futures after shutdown");
    const globals = [];
    const payload = pickle.dumps(PyTuple.from_iterable([fn, PyTuple.from_iterable(args)]), { collect_globals: globals });
    const fut = new ConcurrentFuture();
    this._queue.push({ id: ++this._counter, fut, payload, globals });
    this._next();
    return fut;
  }

  _idle_worker() {
    for (const w of this._workers) if (w.busy === null) return w;
    if (this._workers.size < this._max_workers) {
      const w = new _Worker(this, this._workers.size);
      this._workers.add(w);
      return w;
    }
    return null;
  }

  _next() {
    while (this._queue.length) {
      const w = this._idle_worker();
      if (!w) return;
      const item = this._queue.shift();
      if (!item.fut.set_running_or_notify_cancel()) continue;
      w.run(item);
    }
  }

  /** ``map(fn, *iterables)`` -> list of results (blocking). */
  map(fn, ...iterables) {
    const arrs = iterables.map((it) => [...it]);
    const n = Math.min(...arrs.map((a) => a.length));
    const futs = [];
    for (let i = 0; i < n; i++) futs.push(this.submit(fn, ...arrs.map((a) => a[i])));
    return futs.map((f) => f.result());
  }

  _pending() {
    if (this._queue.length) return true;
    for (const w of this._workers) if (w.busy !== null) return true;
    return false;
  }

  /** ``shutdown(wait=True, cancel_futures=False)`` */
  shutdown(opts = {}) {
    const { wait = true, cancel_futures = false } = opts;
    this._shutdown = true;
    if (cancel_futures) {
      for (const item of this._queue) item.fut.cancel();
      this._queue = [];
    }
    if (wait && this._pending()) blocking_wait(() => !this._pending(), null);
    if (!this._pending() || !wait) this._kill_all();
    else {
      // wait=False with work in flight: reap once everything settles
      const check = () => {
        if (!this._pending()) this._kill_all();
        else setTimeout(check, 50).unref();
      };
      check();
    }
  }
  async shutdown_async(opts = {}) {
    const { cancel_futures = false } = opts;
    this._shutdown = true;
    if (cancel_futures) {
      for (const item of this._queue) item.fut.cancel();
      this._queue = [];
    }
    while (this._pending()) await new Promise((r) => setTimeout(r, 10));
    this._kill_all();
  }
  _kill_all() {
    for (const w of [...this._workers]) w.kill();
    this._workers.clear();
  }
  __enter__() {
    return this;
  }
  __exit__() {
    this.shutdown({ wait: true });
    return false;
  }
}
