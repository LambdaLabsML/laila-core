/**
 * Base schema for the central command sub-system.
 *
 * Central command is the policy's submission hub. It owns:
 *
 * - a registry of ``_LAILA_IDENTIFIABLE_TASK_FORCE`` instances (keyed by gid)
 *   plus a designated ``alpha_taskforce`` that receives work when the caller
 *   does not specify a target;
 * - a thread-local *guarantee stack* used by ``with laila.guarantee:`` to
 *   record every future created inside a scope so the scope can block on
 *   them all at exit.
 *
 * The ``submit`` method is the single entry point for all queued work. Sync
 * callables submitted through it are auto-wrapped into trivial coroutine
 * functions so the runner sees a uniform contract -- no separate "sync vs
 * async submit" plumbing is needed downstream.
 */
import { NotImplementedError } from "../../../../_compat/errors.js";
import { lazy, register } from "../../../../_compat/lazy.js";
import { PrivateAttr, define_fields, define_private } from "../../../../_compat/pydantic.js";
import { dict_has, dict_keys, dict_len, dict_set, dict_values, getitem } from "../../../../_compat/pytypes.js";
import { local as threading_local } from "../../../../_compat/threading.js";
import { CLICapable, CLIExempt } from "../../../../basics/definitions/cli_capable.js";
import { _LAILA_IDENTIFIABLE_OBJECT } from "../../../../basics/definitions/identifiable_object.js";
import { _CENTRAL_COMMAND_SCOPE } from "../../../../macros/strings.js";
import { _shutdown_args, _submit_args } from "../taskforce/base.js";
import { NestedCommandSubmitError, _check_no_pending_submit_owner, ensure_coroutine_function } from "./exceptions.js";
import { _CURRENT_SLOT } from "./parking.js";

/**
 * Central command -- task-force registry, work submission, and guarantee scopes.
 *
 * Owns
 * ----
 * - ``taskforces`` : ``{gid: TaskForce}`` -- every registered taskforce.
 * - ``alpha_taskforce`` : the gid of the default (user-facing) taskforce.
 * - ``internal_taskforce`` : the gid of the taskforce laila's own machinery
 *   (memory fetches/writes, manifest realisation) runs on. Both are created
 *   automatically by ``model_post_init`` if no taskforces were provided.
 * - ``policy_id`` : back-reference to the owning policy's gid.
 *
 * Provides
 * --------
 * - ``submit`` -- the universal submission API.
 * - ``shutdown`` -- shuts down every registered taskforce.
 * - The thread-local guarantee stack hooks (``_guarantee_enter``,
 *   ``_guarantee_exit``, ``_register_future_with_active_guarantees``) used by
 *   the ``laila.guarantee`` context manager.
 */
export class _LAILA_IDENTIFIABLE_CENTRAL_COMMAND extends CLICapable(_LAILA_IDENTIFIABLE_OBJECT) {
  static {
    define_private(this, {
      _scopes: PrivateAttr({ default_factory: () => [_CENTRAL_COMMAND_SCOPE] }),
      _guarantee_local: PrivateAttr({ default_factory: () => threading_local() }),
    });
    define_fields(this, {
      taskforces: ["dict[str, Any]", CLIExempt({ default_factory: () => ({}) })],
      alpha_taskforce: ["str | None", null],
      internal_taskforce: ["str | None", null],
      policy_id: [[_LAILA_IDENTIFIABLE_OBJECT, "str", "None"], CLIExempt({ default: null })],
    });
  }

  /**
   * Create the default taskforces if none were registered.
   *
   * When ``taskforces`` is empty, auto-creates two ``DefaultTaskForce``
   * instances (``PythonAsyncThreadPoolTaskForce``):
   *
   * - the **alpha** taskforce (``rank=2``) receives user work submitted
   *   without an explicit ``taskforce_id``;
   * - the **internal** taskforce (``rank=1``) runs laila's own machinery
   *   (``remember`` / ``memorize`` / ``forget`` fetches, manifest
   *   realisation, ``laila.build``).
   *
   * The split keeps a flood of user jobs from starving the internal fetches
   * those very jobs depend on, and the rank guard in ``submit`` keeps the
   * dependency between the two acyclic. Users can still register additional
   * taskforces (e.g. process pools) via ``add_taskforce``. When the registry
   * is restored from an environment that predates the split (one taskforce),
   * ``internal_taskforce`` falls back to ``alpha_taskforce``.
   */
  model_post_init(_context) {
    super.model_post_init(_context);
    if (dict_len(this.taskforces) === 0) {
      const { DefaultTaskForce } = lazy("laila.macros.defaults");

      const alpha = new DefaultTaskForce({ policy_id: this.policy_id, rank: 2 });
      dict_set(this.taskforces, alpha.global_id, alpha);
      this.alpha_taskforce = alpha.global_id;

      const internal = new DefaultTaskForce({ policy_id: this.policy_id, rank: 1 });
      dict_set(this.taskforces, internal.global_id, internal);
      this.internal_taskforce = internal.global_id;
    }

    if (this.alpha_taskforce === null || !dict_has(this.taskforces, this.alpha_taskforce)) {
      this.alpha_taskforce = dict_keys(this.taskforces)[0];
    }
    if (this.internal_taskforce === null || !dict_has(this.taskforces, this.internal_taskforce)) {
      this.internal_taskforce = this.alpha_taskforce;
    }

    return this;
  }

  /**
   * Register a task-force with this central command.
   *
   * Subsequent ``submit`` calls can target it via ``taskforce_id=<gid>``.
   * Re-registering the same gid silently overwrites the previous instance,
   * which is sometimes useful when hot-swapping a taskforce implementation.
   *
   * @param {any} taskforce The ``_LAILA_IDENTIFIABLE_TASK_FORCE`` instance to register.
   */
  add_taskforce(taskforce) {
    dict_set(this.taskforces, taskforce.global_id, taskforce);
  }

  /**
   * Return the thread-local stack of open guarantee scopes.
   *
   * The stack lives on a ``threading.local`` so each thread has its own
   * independent set of scopes -- nested ``with laila.guarantee:`` blocks in
   * one thread don't see futures created on another. Created lazily on first
   * access.
   * @returns {Array<Record<string, any>>}
   */
  _guarantee_stack() {
    let stack = this._guarantee_local.stack ?? null;
    if (stack === null) {
      stack = [];
      this._guarantee_local.stack = stack;
    }
    return stack;
  }

  /**
   * Push a fresh empty scope onto the calling thread's guarantee stack.
   *
   * Called by the ``laila.guarantee`` context manager on ``__enter__``.
   * Subsequent futures created on this thread are registered into this scope
   * by ``_register_future_with_active_guarantees`` until the scope is popped.
   */
  _guarantee_enter() {
    this._guarantee_stack().push({});
  }

  /**
   * Pop the top guarantee scope and return its accumulated futures.
   *
   * Returns ``[]`` (rather than raising) when no scope is open, so that
   * nested context-manager unwinding paths are robust to partial setup.
   */
  _guarantee_exit() {
    const stack = this._guarantee_stack();
    if (!stack.length) return [];
    return dict_values(stack.pop());
  }

  /**
   * Record *future* in every guarantee scope open on this thread.
   *
   * Called by every ``Future`` / ``GroupFuture`` inside ``model_post_init``
   * so that any future created while ``with laila.guarantee:`` is in effect
   * is automatically tracked for the scope's join-on-exit pass. No-op when no
   * scopes are open (the common case).
   */
  _register_future_with_active_guarantees(future) {
    const stack = this._guarantee_local.stack ?? null;
    if (!stack || !stack.length) return;

    for (const scope_futures of stack) dict_set(scope_futures, future.global_id, future);
  }

  /**
   * Submit zero-argument callables to a task-force for execution.
   *
   * Calling protocol
   * ----------------
   * - With one task and ``wait=False``: returns a single ``Future``.
   * - With many tasks and ``wait=False``: returns a ``GroupFuture``
   *   aggregating per-task futures.
   * - With ``wait=True``: blocks until all tasks finish, then returns the
   *   lone result (one task) or a list of results (many tasks).
   *
   * Sync callables are auto-wrapped into coroutine functions at submission
   * time so the runner sees a uniform awaitable contract; the wrapped sync
   * body is offloaded to the taskforce's sync executor so it never blocks a
   * loop thread.
   *
   * Nested submission
   * -----------------
   * Submitting from *inside* a running task is supported and deadlock-free:
   * while the parent awaits (or blocks in ``wait()`` on) the child future its
   * taskforce slot is parked, so the child can always be dispatched -- see
   * ``laila.policy.central.command.schema.parking``.
   *
   * Guards
   * ------
   * - Calling ``submit`` from inside a function decorated
   *   ``no_command_submit`` raises ``NestedCommandSubmitError``.
   * - Submitting from a task running on taskforce *A* to a taskforce *B*
   *   with a strictly higher ``rank`` raises ``NestedCommandSubmitError``:
   *   dependencies between taskforces must point downward (user -> internal),
   *   never upward, so the inter-taskforce wait-for graph stays acyclic.
   *
   * @param {Iterable<Function>} tasks Zero-arg callables. Sync and async
   *   callables are both accepted -- the wrapper does the right thing per task.
   * @param {boolean|{wait?: boolean, taskforce_id?: string|null}} [wait=false]
   *   ``wait``: if true, block until all tasks complete and return their
   *   results synchronously. ``taskforce_id``: target task-force gid
   *   (defaults to the alpha task-force).
   * @returns {any} A future-like handle when ``wait=False``; a result or list
   *   of results when ``wait=True``.
   * @throws {NestedCommandSubmitError} If invoked from inside a function
   *   decorated ``no_command_submit``.
   * @throws {KeyError} If ``taskforce_id`` is supplied but not registered.
   */
  submit(tasks, wait = false) {
    const opts = _submit_args(wait);
    let taskforce_id = opts.taskforce_id ?? null;

    _check_no_pending_submit_owner();

    if (taskforce_id === null) taskforce_id = this.alpha_taskforce;

    const target = getitem(this.taskforces, taskforce_id);
    this._check_rank(target);

    const wrapped = [...tasks].map((t) => ensure_coroutine_function(t));

    return target.submit(wrapped, { wait: opts.wait });
  }

  /**
   * Reject a submission that would point *up* the taskforce ranks.
   *
   * Only applies when the caller is itself running inside a slot of one of
   * this command's taskforces; submissions from user threads are never
   * restricted.
   */
  _check_rank(target) {
    const slot = _CURRENT_SLOT.get();
    if (slot === null) return;
    const source = slot.tf;
    const source_gid = source !== null && source !== undefined ? (source.global_id ?? null) : null;
    if (source === target || !dict_has(this.taskforces, source_gid)) return;
    const src_rank = source.rank ?? 2;
    const dst_rank = target.rank ?? 2;
    if (dst_rank > src_rank) {
      throw new NestedCommandSubmitError(
        `Task running on taskforce ${source.global_id} (rank ${src_rank}) ` +
          `may not submit to taskforce ${target.global_id} (rank ${dst_rank}): ` +
          "submissions must target equal or lower rank so the taskforce " +
          "dependency graph stays acyclic.",
      );
    }
  }

  /**
   * Shut down every registered task-force.
   *
   * Iterates over ``this.taskforces.values()`` and calls
   * ``shutdown(wait, cancel_pending)`` on each. Order is iteration order
   * (currently insertion order). Failures in one taskforce do NOT prevent
   * the others from being shut down -- exceptions propagate after the
   * iteration to the caller, but the ``laila.terminate`` call site catches
   * them so partial teardown still completes.
   *
   * @param {boolean|{wait?: boolean, cancel_pending?: boolean}} [wait=true]
   *   Block until workers finish in-flight tasks.
   * @param {boolean} [cancel_pending=false] Drop tasks that have been queued
   *   but not yet started. Already-running tasks are not cancelled regardless
   *   of this flag.
   */
  shutdown(wait = true, cancel_pending = false) {
    const opts = _shutdown_args(wait, cancel_pending);
    for (const tf of dict_values(this.taskforces)) tf.shutdown({ wait: opts.wait, cancel_pending: opts.cancel_pending });
  }

  /** Async awaiting is not yet supported. */
  __await__() {
    throw new NotImplementedError();
  }
}

register("laila.policy.central.command.schema.base", { _LAILA_IDENTIFIABLE_CENTRAL_COMMAND });
