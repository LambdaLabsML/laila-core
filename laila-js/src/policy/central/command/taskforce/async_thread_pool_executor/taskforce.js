/**
 * Async-thread-pool taskforce -- N owned threads, each running its own asyncio loop.
 *
 * Each loop hosts up to ``max_async_per_thread`` concurrent in-flight tasks
 * (*slots*). Submitted callables must be coroutine functions (zero-arg).
 * Plain sync callables are auto-wrapped (at the ``Command.submit`` boundary
 * and again, idempotently, in ``_queue_submit`` for direct
 * ``taskforce.submit`` callers) into ``async`` shims that offload the body to
 * this taskforce's ``sync`` executor (see ``ensure_coroutine_function``), so
 * a sync body never blocks a loop thread. Async bodies yield on every
 * ``await`` and let the loop interleave other in-flight coroutines.
 *
 * The dispatcher routes each task to the loop with the lowest current
 * in-flight count whose count is strictly below ``max_async_per_thread``;
 * when every loop is at the cap, the dispatcher blocks until one frees up.
 *
 * Slot parking
 * ------------
 * A task that awaits a laila future while holding a slot *parks*: the slot
 * is released for the duration of the wait and re-acquired afterwards (see
 * ``laila.policy.central.command.schema.parking``). Parked tasks are resumed
 * with priority over freshly dispatched roots -- a loop thread hands a freed
 * slot to a waiting parked task before it decrements its in-flight count.
 * This makes nested submissions (``await laila.remember`` inside a submitted
 * coroutine, constitution bodies realising manifests, ...) deadlock-free
 * regardless of ``num_workers`` and ``max_async_per_thread``.
 *
 * Sync offload
 * ------------
 * Sync bodies (auto-wrapped sync callables and constitution bodies) run via
 * ``run_sync`` on an owned executor whose *thread count* is effectively
 * unbounded, while the number of bodies *executing* at any time is bounded by
 * ``sync_workers`` compute permits (a semaphore). A body that blocks on a
 * laila future releases both its slot and its permit (``park_sync``), so deep
 * nesting of blocking sync bodies can never exhaust the pool -- a bounded
 * thread pool would reintroduce the hold-and-wait pattern one level down --
 * while CPU parallelism stays capped at a sane level. The executor is also
 * installed as each loop's default executor so ``asyncio.to_thread`` inside
 * user coroutines lands on it (without a permit).
 *
 * Each ``_LoopThread`` registers its thread id with ``_ASYNC_LOOP_THREAD_IDS``
 * while running so the future ``wait()`` guard can detect (and reject)
 * blocking waits called from inside a loop thread.
 *
 * Reuses ``ConcurrentPackageFuture`` for the per-task future state; the runner
 * coroutine sets ``fut.exception``, ``fut.result``, and ``fut.status``
 * directly.
 *
 * Node mapping
 * ------------
 * There is one JS thread. A *loop thread* is an execution context
 * (``_compat/contextvars.Context``) with its own thread identity; the runner
 * coroutines it hosts are ``asyncio.Task``s created inside that context, so
 * ``threading.get_ident()`` inside a task is the loop's id exactly as in
 * CPython. The dispatcher is a daemon ``Thread`` whose body is a coroutine
 * (it must never block the one real thread), and executor "threads" are
 * macrotask hops (``_compat/executor.js``) so sync bodies may block through
 * the native pump.
 */
import os from "node:os";

import { RuntimeError, ValueError } from "../../../../../_compat/errors.js";
import { register } from "../../../../../_compat/lazy.js";
import * as asyncio from "../../../../../_compat/asyncio.js";
import { with_ } from "../../../../../_compat/contextlib.js";
import { Context, copy_context, current_context } from "../../../../../_compat/contextvars.js";
import { ThreadPoolExecutor, ConcurrentFuture } from "../../../../../_compat/executor.js";
import { ConfigDict, Field, PrivateAttr, define_fields, define_private } from "../../../../../_compat/pydantic.js";
import { Condition, Event, Lock, Semaphore, Thread, blocking_wait, with_lock } from "../../../../../_compat/threading.js";
import { hop } from "../../../../../_compat/pump.js";
import { _register_async_loop_thread, _unregister_async_loop_thread, ensure_coroutine_function } from "../../schema/exceptions.js";
import { FutureStatus } from "../../schema/future/future/future_status.js";
import { GroupFuture } from "../../schema/future/future/group_future.js";
import { _CURRENT_PERMIT, _CURRENT_SLOT, _RESOLVE_CHAIN, _Permit, _SlotCtx } from "../../schema/parking.js";
import { _LAILA_IDENTIFIABLE_TASK_FORCE, _shutdown_args, _submit_args } from "../base.js";
import { TaskForceStatus } from "../status.js";
import { ConcurrentPackageFuture } from "../thread_pool_executor/future.js";

const _CHAIN_KW = "_laila_resolve_chain";
const _EXECUTOR_MAX_WORKERS = 1_000_000;

const _cpu_count = () => os.cpus().length || 1;

/**
 * Minimal ``asyncio.AbstractEventLoop`` surface owned by a ``_LoopThread``.
 *
 * Node has a single event loop; this handle scopes task creation to the owning
 * loop thread's context and exposes the few loop methods the taskforce and the
 * futures use.
 */
class _LoopHandle {
  constructor(lt) {
    this._lt = lt;
    this._default_executor = null;
    this._closed = false;
  }
  set_default_executor(executor) {
    this._default_executor = executor;
  }
  /** Schedule *fn* on the loop thread from any context. */
  call_soon_threadsafe(fn, ...args) {
    if (this._closed) throw new RuntimeError("Event loop is closed");
    hop(() => this._lt._ctx.run(fn, ...args));
    return { cancel() {} };
  }
  call_soon(fn, ...args) {
    return this.call_soon_threadsafe(fn, ...args);
  }
  create_future() {
    return new asyncio.Future();
  }
  create_task(coro, opts) {
    return this._lt._ctx.run(() => asyncio.create_task(coro, opts));
  }
  run_in_executor(executor, fn, ...args) {
    return asyncio.wrap_future((executor ?? this._default_executor ?? { submit: (f, ...a) => asyncio.to_thread(f, ...a) }).submit(fn, ...args));
  }
  stop() {
    this._lt._request_stop();
  }
  close() {
    this._closed = true;
  }
  is_running() {
    return !this._lt._stopped;
  }
  is_closed() {
    return this._closed;
  }
  time() {
    return performance.now() / 1000;
  }
}

/**
 * ``threading.Thread``-like handle for a loop thread (``name`` / ``ident`` /
 * ``is_alive`` / ``join``). The loop "thread" is an execution context driven
 * by the Node event loop, so ``join`` blocks (pumps) until the loop has
 * stopped.
 */
class _LoopThreadHandle {
  constructor(lt, name) {
    this._lt = lt;
    this.name = name;
    this.daemon = true;
  }
  get ident() {
    return this._lt._thread_ident;
  }
  is_alive() {
    return !this._lt._stopped;
  }
  join(timeout = null) {
    if (this._lt._stopped) return;
    if (timeout !== null && timeout !== undefined && timeout <= 0) return;
    blocking_wait(() => this._lt._stopped, timeout);
  }
  async join_async(timeout = null) {
    const deadline = timeout === null || timeout === undefined ? null : performance.now() + timeout * 1000;
    while (!this._lt._stopped) {
      if (deadline !== null && performance.now() >= deadline) return;
      await new Promise((r) => hop(r));
    }
  }
}

/**
 * A single owned thread running its own asyncio event loop.
 *
 * Concurrency budget per loop is tracked via ``_inflight`` against ``cap``;
 * the dispatcher calls ``try_reserve`` to claim a slot for a new root task,
 * runners and parked tasks call ``release_slot`` and ``try_reserve_or_wait``.
 *
 * Slot hand-off
 * -------------
 * ``_resume_waiters`` holds callbacks of parked tasks waiting to get a slot
 * back. ``release_slot`` pops the oldest waiter *instead of* decrementing the
 * in-flight count, so the slot is transferred directly to that parked task
 * and the dispatcher never sees it as free. Only when nobody is waiting is
 * the count decremented and ``true`` returned so the caller can notify the
 * dispatcher's capacity condition.
 *
 * Registers its thread id with the global ``_ASYNC_LOOP_THREAD_IDS`` set
 * while running so blocking ``Future.wait()`` calls from inside the loop can
 * be rejected with ``LoopBlockingWaitError``.
 */
export class _LoopThread {
  /**
   * @param {string} name
   * @param {number} cap
   */
  constructor(name, cap) {
    this.name = name;
    this.cap = cap;
    this._inflight = 0;
    this._inflight_lock = new Lock();
    /** @type {Array<() => void>} */
    this._resume_waiters = [];
    /**
     * Sync waiters handed a slot that have not resumed yet (Node: see
     * ``try_reserve_or_wait``).
     * @type {Set<() => void>}
     */
    this._in_transit = new Set();
    this._ready = new Event();
    // The loop "thread": a fresh execution context with its own identity.
    this._ctx = new Context(current_context(), name);
    this._thread_ident = this._ctx.thread_ident;
    /** @type {Set<asyncio.Task>} ``asyncio.all_tasks(loop)`` */
    this._tasks = new Set();
    /**
     * Synchronous cancellation hooks, keyed by task. CPython's loop teardown
     * runs each cancelled task's ``except CancelledError`` block before
     * ``run_forever`` returns; JS cannot interrupt a coroutine, so the runner
     * registers the equivalent bookkeeping here and ``_finish`` runs it
     * synchronously after ``task.cancel()``.
     * @type {Map<asyncio.Task, () => void>}
     */
    this._cancel_hooks = new Map();
    this._stop_requested = false;
    this._drain = false;
    this._stopped = false;
    this.loop = new _LoopHandle(this);
    this.thread = new _LoopThreadHandle(this, name);
    this._run();
  }

  _run() {
    // ``loop.run_forever()`` -- the Node event loop *is* the loop; mark the
    // context as a loop thread for the blocking-wait guard and signal ready.
    _register_async_loop_thread(this._thread_ident);
    this._ready.set();
  }

  /** ``loop.stop()`` requested; finish once the task set allows it. */
  _request_stop() {
    this._stop_requested = true;
    this._maybe_finish();
  }

  _maybe_finish() {
    if (this._stopped || !this._stop_requested) return;
    if (this._drain && this._tasks.size > 0) return;
    this._finish();
  }

  _finish() {
    if (this._stopped) return;
    this._stopped = true;
    // Equivalent of ``_run``'s ``finally``: cancel whatever is still pending
    // (cancelled tasks unwind their ``finally`` blocks -- parking exit,
    // runner bookkeeping -- on their own), close the loop and unregister.
    for (const t of [...this._tasks]) {
      try {
        t.cancel();
      } catch {
        /* already done */
      }
      const hook = this._cancel_hooks.get(t);
      if (hook) {
        this._cancel_hooks.delete(t);
        try {
          hook();
        } catch {
          /* best effort */
        }
      }
    }
    this.loop.close();
    if (this._thread_ident !== null) _unregister_async_loop_thread(this._thread_ident);
  }

  // ---------- slot accounting ----------
  inflight_count() {
    return with_lock(this._inflight_lock, () => this._inflight);
  }

  /** Number of parked tasks currently waiting to re-acquire a slot. */
  parked_waiting() {
    return with_lock(this._inflight_lock, () => this._resume_waiters.length);
  }

  /** Claim a slot for a new root task if under the cap. */
  try_reserve() {
    return with_lock(this._inflight_lock, () => {
      if (this._inflight >= this.cap) return false;
      this._inflight += 1;
      return true;
    });
  }

  /**
   * Claim a slot now (``true``) or enqueue *cb* to be handed one later.
   *
   * Used by parked tasks re-acquiring. The callback is invoked -- from
   * whichever thread releases the slot -- exactly once when a slot has been
   * transferred to the waiter.
   */
  try_reserve_or_wait(cb) {
    return with_lock(this._inflight_lock, () => {
      if (this._inflight < this.cap) {
        this._inflight += 1;
        return true;
      }
      // Node: a *sync* waiter (``_park_exit_sync`` on an executor body)
      // blocks by pumping the loop underneath itself. A shallower sync
      // waiter that was handed a slot but has not resumed yet is therefore
      // buried under this one and cannot use that slot until we return --
      // which may itself need the slot. Take it over: the buried waiter
      // goes back to the head of the queue and is served again later.
      // CPython's threads resume independently, so there this is a plain
      // FIFO wait.
      if (cb._sync_depth !== undefined) {
        let victim = null;
        for (const other of this._in_transit) {
          if (other._sync_depth < cb._sync_depth && (victim === null || other._sync_depth > victim._sync_depth)) victim = other;
        }
        if (victim !== null) {
          this._in_transit.delete(victim);
          victim._revoke();
          this._resume_waiters.unshift(victim);
          return true;
        }
      }
      this._resume_waiters.push(cb);
      return false;
    });
  }

  /** Remove *cb* from the waiters. ``false`` if it was already served. */
  cancel_wait(cb) {
    return with_lock(this._inflight_lock, () => {
      const i = this._resume_waiters.indexOf(cb);
      if (i < 0) return false;
      this._resume_waiters.splice(i, 1);
      return true;
    });
  }

  /** A served sync waiter has resumed with its slot (Node bookkeeping). */
  waiter_resumed(cb) {
    with_lock(this._inflight_lock, () => this._in_transit.delete(cb));
  }

  /**
   * Oldest waiter that can actually take the slot: coroutine waiters
   * always can; of the *sync* waiters (executor bodies blocked in
   * ``_park_exit_sync``, each pumping the loop underneath the previous one)
   * only the deepest can -- the others sit below it on the stack and would
   * hold a slot they cannot use until it returns. CPython's threads need no
   * such care, so there this is plain FIFO.
   */
  _next_waiter_index() {
    let deepest = -1;
    for (const cb of this._resume_waiters) {
      const d = cb._sync_depth;
      if (d !== undefined && d > deepest) deepest = d;
    }
    for (let i = 0; i < this._resume_waiters.length; i++) {
      const d = this._resume_waiters[i]._sync_depth;
      if (d === undefined || d === deepest) return i;
    }
    return 0;
  }

  /**
   * Give up one slot.
   *
   * Returns ``true`` if the in-flight count was decremented (the dispatcher
   * may now place a new root), ``false`` if the slot was transferred to a
   * parked waiter instead.
   */
  release_slot() {
    const cb = with_lock(this._inflight_lock, () => {
      if (this._resume_waiters.length) {
        const next = this._resume_waiters.splice(this._next_waiter_index(), 1)[0];
        if (next._sync_depth !== undefined) this._in_transit.add(next);
        return next;
      }
      if (this._inflight > 0) this._inflight -= 1;
      return null;
    });
    if (cb !== null) {
      cb();
      return false;
    }
    return true;
  }

  /**
   * ``asyncio.run_coroutine_threadsafe(coro, loop)``: schedule *coro* (a
   * coroutine function, or a promise) as a task on this loop thread and
   * return a ``concurrent.futures.Future`` for its outcome.
   */
  submit_coro(coro) {
    if (this._stopped) throw new RuntimeError("Event loop is closed");
    const cf = new ConcurrentFuture();
    hop(() => {
      if (this._stopped) {
        if (!cf.done()) cf.set_exception(new RuntimeError("Event loop is closed"));
        return;
      }
      this._ctx.run(() => {
        let task;
        try {
          task = asyncio.create_task(coro);
        } catch (err) {
          if (!cf.done()) cf.set_exception(err);
          return;
        }
        this._tasks.add(task);
        task.add_done_callback(() => {
          this._tasks.delete(task);
          this._cancel_hooks.delete(task);
          this._maybe_finish();
        });
        task.then(
          (v) => {
            if (!cf.done()) cf.set_result(v);
          },
          (e) => {
            if (!cf.done()) cf.set_exception(e);
          },
        );
      });
    });
    return cf;
  }

  /** ``asyncio.all_tasks(loop)`` */
  all_tasks() {
    return new Set(this._tasks);
  }

  /**
   * Stop the loop and join its thread.
   *
   * With ``drain=true`` the loop keeps running until every task currently
   * scheduled on it (runners, parked re-acquires, pending sync offloads) has
   * completed, then stops -- this is what makes ``shutdown(wait=True)``
   * honour its "block until in-flight tasks finish" contract. Without it
   * (``wait=False`` or an explicit ``cancel_pending=True``) the loop stops
   * immediately and ``_finish`` cancels whatever is still pending.
   * @param {number|null} [timeout]
   * @param {{drain?: boolean}} [opts]
   */
  stop(timeout = null, opts = {}) {
    const { drain = false } = opts;
    this._drain = !!drain;
    this._request_stop();
    this.thread.join(timeout);
  }
}

/**
 * TaskForce that owns N threads, each running its own asyncio loop.
 *
 * Concurrency knobs:
 *
 * - ``num_workers`` -- number of owned threads (each with its own loop).
 * - ``max_async_per_thread`` -- maximum concurrent in-flight tasks per loop.
 *   A task consumes a slot from scheduling until the coroutine
 *   returns/raises, *minus* the time it spends parked on laila futures.
 * - ``sync_workers`` -- maximum number of sync bodies *executing*
 *   concurrently on executor threads (bodies blocked on laila futures do not
 *   count).
 */
export class PythonAsyncThreadPoolTaskForce extends _LAILA_IDENTIFIABLE_TASK_FORCE {
  static model_config = ConfigDict({ arbitrary_types_allowed: true });

  static {
    define_fields(this, {
      backend: ["str", Field({ default: "async_threads", description: "Execution backend (async_threads only)." })],
      num_workers: [
        "int",
        Field({
          default_factory: () => Math.max(1, _cpu_count()),
          ge: 1,
          description: "Number of worker threads, each running its own asyncio event loop.",
        }),
      ],
      max_async_per_thread: ["int", Field({ default: 64, ge: 1, description: "Maximum concurrent in-flight tasks per loop thread." })],
      sync_workers: [
        "int",
        Field({
          default_factory: () => Math.max(4, _cpu_count() * 2),
          ge: 1,
          description:
            "Maximum number of plain (non-async) bodies executing concurrently " +
            "on executor threads; bodies parked on laila futures do not count.",
        }),
      ],
    });
    define_private(this, {
      _cv: PrivateAttr({ default: null }),
      _capacity_cv: PrivateAttr({ default: null }),
      _stop: PrivateAttr({ default: null }),
      _dispatcher: PrivateAttr({ default: null }),
      _loops: PrivateAttr({ default_factory: () => [] }),
      _executor: PrivateAttr({ default: null }),
      _compute_permits: PrivateAttr({ default: null }),
    });
  }

  _on_start() {
    if (this.backend.toLowerCase() !== "async_threads") {
      throw new ValueError("PythonAsyncThreadPoolTaskForce supports async_threads only.");
    }

    // ``pause()`` -> ``start()`` resumes the existing machinery; only a cold
    // start (or a start after shutdown) spawns loops and a dispatcher.
    if (this._dispatcher !== null && this._dispatcher.is_alive()) return;

    const tag = this.global_id.slice(-8);
    this._cv = new Condition();
    this._capacity_cv = new Condition();
    this._stop = new Event();
    this._executor = new ThreadPoolExecutor({ max_workers: _EXECUTOR_MAX_WORKERS, thread_name_prefix: `AsyncTF-${tag}-Sync` });
    this._compute_permits = new Semaphore(this.sync_workers);
    this._loops = [];
    for (let i = 0; i < this.num_workers; i++) {
      this._loops.push(new _LoopThread(`AsyncTF-${tag}-Loop-${i}`, this.max_async_per_thread));
    }
    for (const lt of this._loops) lt.loop.set_default_executor(this._executor);
    this._dispatcher = new Thread({ target: () => this._loop(), name: `AsyncTF-${tag}-Dispatcher`, daemon: true });
    this._dispatcher.start();
  }

  _on_pause() {
    // Safe no-op (see the base-class contract): the status flips to
    // ``PAUSED`` so ``_queue_submit`` rejects new work; in-flight tasks keep
    // running and ``start()`` resumes without re-spawning threads.
    return;
  }

  _on_shutdown(wait = true, cancel_pending = true) {
    const opts = _shutdown_args(wait, cancel_pending);
    // ``wait=True`` on its own is a graceful stop: in-flight tasks run to
    // completion (loops drain) before the threads are joined.
    // ``cancel_pending=True`` is an explicit request to drop work, so
    // in-flight coroutines are cancelled as before (``wait`` then only
    // governs whether we join the threads).
    const drain = opts.wait && !opts.cancel_pending;
    if (this._stop !== null) this._stop.set();
    if (this._cv !== null) with_(this._cv, () => this._cv.notify_all());
    if (this._capacity_cv !== null) with_(this._capacity_cv, () => this._capacity_cv.notify_all());

    // Always let the dispatcher exit (it polls every 0.1 s and re-queues an
    // item it popped but could not place) *before* the cancel pass, so that
    // pass sees every undispatched task.
    if (this._dispatcher !== null) this._dispatcher.join(opts.wait ? null : 1.0);

    if (opts.cancel_pending) {
      with_(this._q.atomic("cancel"), () => {
        for (const [, item] of this._q.items()) {
          const kwargs = item[2];
          const fut = kwargs.fut ?? null;
          if (fut === null) continue;
          fut.exception = new RuntimeError("Task canceled before dispatch.");
          fut.status = FutureStatus.CANCELLED;
          fut.result = null;
        }
        this._q.clear();
      });
    }

    for (const lt of this._loops) lt.stop(opts.wait ? null : 0.0, { drain });

    if (this._executor !== null) this._executor.shutdown({ wait: opts.wait, cancel_futures: opts.cancel_pending });
  }

  // =========================================================
  // Sync offload
  // =========================================================

  /**
   * Run ``fn(...args)`` on an executor thread under a compute permit.
   *
   * Must be called from a coroutine running on one of this taskforce's loops.
   * The current context (slot, resolve chain) is propagated so the body can
   * park its slot -- and release its permit -- when it blocks on laila
   * futures. Returns an awaitable ``asyncio.Future``.
   *
   * Node: the permit is acquired asynchronously (an executor thread blocked
   * on the semaphore in CPython) and the body then starts from a macrotask
   * hop so it may block synchronously through the pump.
   */
  run_sync(fn, ...args) {
    const executor = this._executor;
    const permits = this._compute_permits;
    if (executor === null || permits === null) throw new RuntimeError("TaskForce must be running before offloading sync work.");
    // ``asyncio.get_running_loop()`` -- raises outside a coroutine.
    if (asyncio.current_task() === null) throw new RuntimeError("no running event loop");
    const ctx = copy_context();
    const permit = new _Permit(permits);

    const _body = () => {
      _CURRENT_PERMIT.set(permit);
      try {
        return fn(...args);
      } finally {
        permit.release();
      }
    };

    const fut = new asyncio.Future();
    (async () => {
      await permit.acquire_async();
      let cf;
      try {
        cf = executor.submit(() => ctx.run_on_current_thread(_body));
      } catch (err) {
        permit.release();
        throw err;
      }
      return await cf;
    })().then(
      (v) => {
        if (!fut.done()) fut.set_result(v);
      },
      (e) => {
        if (!fut.done()) fut.set_exception(e);
      },
    );
    return fut;
  }

  // =========================================================
  // Observability
  // =========================================================

  /** Slots currently occupied across all loops. */
  get inflight() {
    return this._loops.reduce((acc, lt) => acc + lt.inflight_count(), 0);
  }

  /** Parked tasks currently waiting to re-acquire a slot. */
  get parked() {
    return this._loops.reduce((acc, lt) => acc + lt.parked_waiting(), 0);
  }

  // =========================================================
  // Submission
  // =========================================================

  _queue_submit(task, ...args) {
    if (this.status !== TaskForceStatus.RUNNING) throw new RuntimeError("TaskForce must be running before submitting tasks.");

    // Direct ``taskforce.submit`` / ``imap`` callers bypass ``Command.submit``;
    // wrap here as well (idempotent) so a plain sync body is offloaded to the
    // executor instead of blocking a loop thread.
    task = ensure_coroutine_function(task);

    const fut = new ConcurrentPackageFuture({ taskforce_id: this.global_id, policy_id: this.policy_id });

    const kwargs = {};
    with_(this._cv, () => {
      with_(this._q.atomic(), () => {
        kwargs.task = task;
        kwargs.fut = fut;
        // Snapshot the caller's resolve chain so the child root inherits it
        // even though it runs on another loop thread.
        kwargs[_CHAIN_KW] = _RESOLVE_CHAIN.get();
        this._q.__setitem__(fut.global_id, [null, args, kwargs]);
      });
      this._cv.notify();
    });

    return fut;
  }

  *imap(tasks) {
    // Yield the future itself: it already *is* a _LAILA_IDENTIFIABLE_FUTURE,
    // so building a separate identity handle per task is pure overhead.
    for (const f of tasks) yield this._queue_submit(f);
  }

  /**
   * @param {Iterable<Function>} tasks
   * @param {boolean|{wait?: boolean}} [wait=false]
   */
  submit(tasks, wait = false) {
    const opts = _submit_args(wait);
    tasks = [...tasks];

    const futures = [];
    for (const task of tasks) {
      const fut = this._queue_submit(task);
      fut.taskforce_id = this.global_id;
      futures.push(fut);
    }

    if (futures.length === 1) {
      const single = futures[0];
      if (opts.wait) return single.wait(null);
      // Return the concrete future (an identity-compatible object) rather
      // than a fresh identity handle; accessors resolve directly instead of
      // scanning every local policy's bank.
      return single;
    }

    const gf = new GroupFuture({
      taskforce_id: this.global_id,
      policy_id: this.policy_id,
      future_ids: futures.map((f) => f.global_id),
    });

    for (const f of futures) f.future_group_id = gf.global_id;

    if (!opts.wait) return gf;
    return gf.wait(null);
  }

  // =========================================================
  // Dispatcher
  // =========================================================

  /** Reserve a slot on the loop with the lowest in-flight count, or null. */
  _pick_loop() {
    const cap = this.max_async_per_thread;
    let best = null;
    let best_count = cap;
    for (const lt of this._loops) {
      const c = lt.inflight_count();
      if (c < best_count) {
        best_count = c;
        best = lt;
      }
    }
    if (best !== null && best.try_reserve()) return best;
    return null;
  }

  async _loop() {
    const cv = this._cv;
    const cap_cv = this._capacity_cv;
    const stop = this._stop;

    while (!stop.is_set()) {
      // ``with cv:`` -- the check and the pop are one synchronous step, so
      // the lock is only ever held synchronously (see ``wait_unlocked_async``).
      while (!stop.is_set() && this._q.__len__() === 0) await cv.wait_unlocked_async(0.1, { unref: true });
      if (stop.is_set()) break;
      const item = with_(cv, () => this._q.pop_next()[1]);
      const [, args, kwargs] = item;

      let picked = null;
      while (!stop.is_set() && picked === null) {
        picked = this._pick_loop();
        if (picked !== null) break;
        await cap_cv.wait_unlocked_async(0.1, { unref: true });
      }

      if (stop.is_set()) {
        with_(this._q.atomic(), () => {
          this._q.__setitem__(kwargs.fut.global_id, [null, args, kwargs]);
        });
        break;
      }

      const task = kwargs.task;
      const fut = kwargs.fut;
      const chain = kwargs[_CHAIN_KW] ?? [];
      const user_kwargs = {};
      for (const [k, v] of Object.entries(kwargs)) if (k !== "task" && k !== "fut" && k !== _CHAIN_KW) user_kwargs[k] = v;

      try {
        const coro = this._make_runner_coro(task, args, user_kwargs, fut, picked, chain);
        picked.submit_coro(coro);
      } catch (exc) {
        if (picked.release_slot()) with_(cap_cv, () => cap_cv.notify());
        fut.exception = exc;
        fut.result = null;
        fut.status = FutureStatus.ERROR;
      }
    }
  }

  /**
   * Build the runner coroutine for one task.
   *
   * Returns the coroutine *function* (un-started): ``_LoopThread.submit_coro``
   * starts it inside the loop thread's context, like
   * ``run_coroutine_threadsafe`` does with a coroutine object.
   */
  _make_runner_coro(task, args, kwargs, fut, lt, chain = []) {
    const cap_cv = this._capacity_cv;

    const _runner = async () => {
      const ctx = new _SlotCtx(this, lt);
      const slot_token = _CURRENT_SLOT.set(ctx);
      const chain_token = _RESOLVE_CHAIN.set(Object.freeze([...chain]));
      const me = asyncio.current_task();
      // JS cannot interrupt a coroutine: when the task is cancelled (loop
      // teardown) the future is marked right away, exactly where CPython's
      // ``except asyncio.CancelledError`` would land once the ``await`` wakes.
      const _mark_cancelled = () => {
        if (fut.status === FutureStatus.CANCELLED) return;
        fut.exception = new RuntimeError("Task cancelled during taskforce shutdown.");
        fut.result = null;
        fut.status = FutureStatus.CANCELLED;
      };
      if (me !== null) {
        lt._cancel_hooks.set(me, _mark_cancelled);
        me.add_done_callback((t) => {
          if (t.cancelled()) _mark_cancelled();
        });
      }
      fut.status = FutureStatus.RUNNING;
      try {
        let out = Object.keys(kwargs).length ? task(...args, kwargs) : task(...args);
        if (asyncio.iscoroutine(out)) out = await out;
        if (me !== null && me.cancelled()) throw new asyncio.CancelledError();
        fut.exception = null;
        fut.result = out;
        fut.status = FutureStatus.FINISHED;
      } catch (exc) {
        if (exc instanceof asyncio.CancelledError || (me !== null && me.cancelled())) {
          _mark_cancelled();
          throw exc instanceof asyncio.CancelledError ? exc : new asyncio.CancelledError();
        }
        fut.exception = exc;
        fut.result = null;
        fut.status = FutureStatus.ERROR;
      } finally {
        _RESOLVE_CHAIN.reset(chain_token);
        _CURRENT_SLOT.reset(slot_token);
        const holds = with_lock(ctx.lock, () => {
          const h = ctx.holds_slot;
          ctx.holds_slot = false;
          return h;
        });
        if (holds && lt.release_slot()) with_(cap_cv, () => cap_cv.notify());
      }
    };

    return _runner;
  }
}

register("laila.policy.central.command.taskforce.async_thread_pool_executor.taskforce", {
  _LoopThread,
  PythonAsyncThreadPoolTaskForce,
});
