/**
 * Abstract base task-force with lifecycle management and a submission queue.
 *
 * A *task-force* is laila's name for a worker pool. Concrete subclasses
 * (``PythonAsyncThreadPoolTaskForce``, ``PythonProcessPoolTaskForce``)
 * implement the actual scheduling -- this base just nails down:
 *
 * - a uniform ``TaskForceStatus`` lifecycle
 *   (NOT_STARTED -> RUNNING -> [PAUSED] -> STOPPED);
 * - a process-wide registry of *live* taskforces (used by
 *   ``laila.terminate`` to sweep orphans -- those not reachable through any
 *   policy);
 * - a uniform ``submit`` / ``imap`` contract (subclass-defined);
 * - context-manager sugar so a task-force can be used as ``with tf: ...``
 *   (``with_(tf, () => ...)`` / ``using``);
 * - a ``_q`` ``AtomicDict`` slot subclasses can repurpose as a per-task-force
 *   submission queue.
 *
 * Lifecycle invariants
 * --------------------
 * - ``model_post_init`` calls ``start`` for any taskforce constructed in the
 *   ``NOT_STARTED`` state, so freshly-instantiated taskforces are immediately
 *   usable.
 * - ``start`` and ``shutdown`` only flip the status flag *after* the
 *   subclass-specific hook (``_on_start`` / ``_on_shutdown``) returns, so
 *   partial transitions cannot leave a taskforce in an inconsistent state.
 */
import { NotImplementedError } from "../../../../_compat/errors.js";
import { register } from "../../../../_compat/lazy.js";
import { ConfigDict, Field, PrivateAttr, define_fields, define_private } from "../../../../_compat/pydantic.js";
import { Lock, with_lock } from "../../../../_compat/threading.js";
import { AtomicDict } from "../../../../atomic/index.js";
import { _LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT } from "../../../../atomic/definitions/locally_atomic_identifiable_object.js";
import { CLICapable, CLIExempt } from "../../../../basics/definitions/cli_capable.js";
import { _LAILA_IDENTIFIABLE_OBJECT } from "../../../../basics/definitions/identifiable_object.js";
import { _TASK_FORCE_SCOPE } from "../../../../macros/strings.js";
import { TaskForceStatus } from "./status.js";

/** @type {Set<any>} */
export const _LIVE_TASKFORCES = new Set();
const _LIVE_TASKFORCES_LOCK = new Lock();

/**
 * Track *tf* in the process-wide live set.
 *
 * The set is consumed by ``laila.terminate`` to sweep "orphan" task-forces --
 * those constructed directly (e.g. ``TaskForce.process_pool()`` in user code)
 * and never attached to a policy. Without this set those would leak their
 * worker threads / processes through to interpreter shutdown.
 *
 * Thread-safe via ``_LIVE_TASKFORCES_LOCK``.
 */
export function _register_live_taskforce(tf) {
  with_lock(_LIVE_TASKFORCES_LOCK, () => {
    _LIVE_TASKFORCES.add(tf);
  });
}

/**
 * Remove *tf* from the live set. Best-effort; idempotent.
 *
 * Called during ``_LAILA_IDENTIFIABLE_TASK_FORCE.shutdown``.
 */
export function _unregister_live_taskforce(tf) {
  with_lock(_LIVE_TASKFORCES_LOCK, () => {
    _LIVE_TASKFORCES.delete(tf);
  });
}

/**
 * Return a snapshot list of currently-registered live task-forces.
 *
 * The snapshot is detached from the live set, so callers can iterate without
 * holding the registry lock and without worrying about concurrent mutation.
 */
export function _live_taskforces_snapshot() {
  return with_lock(_LIVE_TASKFORCES_LOCK, () => [..._LIVE_TASKFORCES]);
}

/**
 * Normalise ``shutdown(wait=True, cancel_pending=False)`` arguments: accepts
 * the positional Python form (``shutdown(false, true)``) as well as the
 * trailing-options form (``shutdown({ wait: false, cancel_pending: true })``).
 * @returns {{wait: boolean, cancel_pending: boolean}}
 */
export function _shutdown_args(wait = true, cancel_pending = false) {
  if (wait !== null && typeof wait === "object") {
    return { wait: wait.wait ?? true, cancel_pending: wait.cancel_pending ?? false };
  }
  return { wait: !!wait, cancel_pending: !!cancel_pending };
}

/**
 * Normalise ``submit(tasks, wait=False, ...)`` keyword arguments: a bare
 * boolean is the positional ``wait``; an object is the options bag.
 * @returns {{wait: boolean, [k: string]: any}}
 */
export function _submit_args(opts = {}) {
  if (typeof opts === "boolean") return { wait: opts };
  if (opts === null || opts === undefined) return { wait: false };
  return { ...opts, wait: !!opts.wait };
}

/**
 * Abstract base for task-forces -- worker-pool registries with a uniform
 * submit / shutdown contract.
 *
 * Concrete subclasses provide the actual scheduling backend (asyncio thread
 * pool, ``ProcessPoolExecutor``, or remote pools). The base class enforces a
 * single status state machine and queue-length introspection so callers can
 * write backend-agnostic code.
 */
export class _LAILA_IDENTIFIABLE_TASK_FORCE extends CLICapable(_LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT) {
  static model_config = ConfigDict({ arbitrary_types_allowed: true });

  static {
    define_private(this, {
      _scopes: PrivateAttr({ default_factory: () => [_TASK_FORCE_SCOPE] }),
      // ---- Shared runtime state (available to subclasses) ----
      _q: PrivateAttr({ default_factory: () => new AtomicDict() }),
    });
    define_fields(this, {
      policy_id: [[_LAILA_IDENTIFIABLE_OBJECT, "str", "None"], CLIExempt({ default: null })],
      rank: [
        "int",
        Field({
          default: 2,
          ge: 0,
          description:
            "Dependency rank. Work running on a taskforce may only submit to " +
            "taskforces of equal or lower rank; the central command rejects " +
            "upward submissions so the wait-for graph between taskforces stays " +
            "acyclic. laila's internal taskforce has rank 1, user-facing ones 2.",
        }),
      ],
      status: [
        TaskForceStatus,
        CLIExempt({
          default: TaskForceStatus.NOT_STARTED,
          description: "Current lifecycle status of this TaskForce.",
        }),
      ],
    });
  }

  /**
   * Auto-start the task-force when constructed in ``NOT_STARTED``.
   *
   * This makes ``new MyTaskForce()`` immediately usable -- callers rarely
   * need to remember a separate ``.start()`` call. To construct without
   * starting, pass ``status: TaskForceStatus.PAUSED`` explicitly.
   */
  model_post_init(_context) {
    super.model_post_init(_context);
    if (this.status === TaskForceStatus.NOT_STARTED) this.start();
  }

  // ---------- Observability ----------
  /**
   * Number of submitted tasks not yet observed as completed.
   *
   * Backed by the per-instance ``AtomicDict`` queue ``_q``; thread-safe by
   * virtue of the underlying atomic-dict locking.
   */
  get queue_len() {
    return this._q.__len__();
  }

  /**
   * ``len(taskforce)`` returns the current queue length, not the worker count.
   *
   * Defined this way so ``if not tf:`` reads naturally as "the taskforce has
   * nothing pending".
   */
  __len__() {
    return Math.trunc(this.queue_len);
  }

  get length() {
    return this.__len__();
  }

  // ---------- Public lifecycle ----------
  /**
   * Start the task-force.
   *
   * Idempotent: a second call on a running task-force is a no-op. Calls the
   * subclass-defined ``_on_start`` hook first; only flips the status to
   * ``RUNNING`` if the hook returns without raising. The taskforce is also
   * added to the process-wide live registry here so ``laila.terminate`` can
   * find it.
   */
  start() {
    if (this.status === TaskForceStatus.RUNNING) return;
    this._on_start();
    this.status = TaskForceStatus.RUNNING;
    _register_live_taskforce(this);
  }

  /**
   * Quiesce the task-force without destroying its resources.
   *
   * Calls ``_on_pause`` and flips the status to ``PAUSED`` -- future
   * submissions are still accepted by some backends but not actively
   * dispatched until ``start`` is called again. No-op when the task-force is
   * not currently ``RUNNING``.
   */
  pause() {
    if (this.status !== TaskForceStatus.RUNNING) return;
    this._on_pause();
    this.status = TaskForceStatus.PAUSED;
  }

  /**
   * Tear down the task-force and release its resources.
   *
   * Idempotent: a second call on a stopped task-force still unregisters it
   * from the live set (cheap) and returns. The subclass-defined
   * ``_on_shutdown`` hook runs inside a ``try`` so the status flag is
   * *always* updated to ``STOPPED`` and the live-set registration removed,
   * even if the hook raises.
   *
   * @param {boolean|{wait?: boolean, cancel_pending?: boolean}} [wait=true]
   *   Block until in-flight tasks finish before returning.
   * @param {boolean} [cancel_pending=false]
   *   Drop tasks that have been queued but not yet started.
   */
  shutdown(wait = true, cancel_pending = false) {
    const opts = _shutdown_args(wait, cancel_pending);
    if (this.status === TaskForceStatus.STOPPED) {
      _unregister_live_taskforce(this);
      return;
    }
    try {
      this._on_shutdown(opts);
    } finally {
      this.status = TaskForceStatus.STOPPED;
      _unregister_live_taskforce(this);
    }
  }

  // Context manager sugar
  /** Enter context: ensure the task-force is running and return ``this``. */
  __enter__() {
    this.start();
    return this;
  }

  /**
   * Exit context: shut down with ``wait=True`` regardless of exception state.
   *
   * Pending tasks are *not* cancelled by default, matching the executor
   * convention from the standard library. Call ``shutdown`` directly if you
   * need different semantics.
   */
  __exit__(_exc_type, _exc, _tb) {
    this.shutdown({ wait: true });
  }

  enter() {
    return this.__enter__();
  }

  exit(exc = null) {
    return this.__exit__(exc === null ? null : exc.constructor, exc, null);
  }

  [Symbol.dispose]() {
    this.__exit__(null, null, null);
  }

  // ---------- Subclass hooks (must implement) ----------
  /**
   * Allocate or start backend resources (worker threads, dispatchers, ...).
   *
   * Called from ``start`` *before* the status flag flips to ``RUNNING``. If
   * this raises, the task-force stays in its previous state and is *not*
   * added to the live registry.
   */
  _on_start() {
    throw new NotImplementedError();
  }

  /**
   * Quiesce backend resources without tearing them down.
   *
   * Called from ``pause``. The default base implementation raises
   * ``NotImplementedError``; backends that cannot meaningfully pause should
   * still implement it as a no-op so ``pause()`` is at least safe to call.
   */
  _on_pause() {
    throw new NotImplementedError();
  }

  /**
   * Tear down backend resources, honouring *wait* and *cancel_pending*.
   *
   * Called from ``shutdown`` inside a ``try``: any exception raised here is
   * allowed to propagate, but the status flag and live-set registration are
   * still cleaned up by the caller.
   * @param {{wait: boolean, cancel_pending: boolean}} _opts
   */
  _on_shutdown(_opts) {
    throw new NotImplementedError();
  }

  // ---------- Mapping / submit API (must implement) ----------
  /**
   * Batch-submit an iterable of zero-arg callables.
   *
   * Subclasses must accept *funcs* of any length and obey the return-shape
   * contract below so callers can write backend-agnostic code.
   *
   * ``len(funcs)`` / ``wait`` -> return type:
   *
   * - ``1`` / ``false``: a single ``Future``
   * - ``> 1`` / ``false``: a grouped/aggregate ``Future`` (e.g. ``GroupFuture``)
   * - ``1`` / ``true``: the single result value
   * - ``> 1`` / ``true``: a list of result values
   *
   * @param {Iterable<Function>} _funcs Zero-arg callables (sync or coroutine
   *   functions, depending on backend) to enqueue.
   * @param {boolean|{wait?: boolean}} [_wait=false] If true, block until all
   *   submissions complete and return the result(s) directly. If false,
   *   return future(s).
   */
  submit(_funcs, _wait = false) {
    throw new NotImplementedError();
  }

  /**
   * Submit *funcs* lazily, yielding ``Future`` instances in submission order.
   *
   * Unlike ``submit``, this is an iterator: each ``next(...)`` triggers the
   * next submission. Useful for streaming pipelines where the iterable is
   * very large or even unbounded.
   *
   * The yielded futures complete in arbitrary order; pair with
   * ``Future.wait`` or ``await`` if you need the results.
   */
  imap(_funcs) {
    throw new NotImplementedError();
  }
}

register("laila.policy.central.command.taskforce.base", {
  _LIVE_TASKFORCES,
  _register_live_taskforce,
  _unregister_live_taskforce,
  _live_taskforces_snapshot,
  _LAILA_IDENTIFIABLE_TASK_FORCE,
});
