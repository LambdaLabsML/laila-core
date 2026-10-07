/**
 * Guarantee context managers that block until tracked futures complete.
 *
 * The "guarantee" pattern is laila's lexical answer to "I want to be sure
 * every async operation kicked off in *this block* has finished before the
 * block returns". Two mirror implementations live here:
 *
 * - ``_Guarantee`` -- synchronous version. Use as
 *   ``with_(laila.guarantee, () => { ... })``. On exit, every future created
 *   *inside* the block is awaited via ``Future.wait``. The first exception
 *   observed is re-raised so the caller never silently outlives a failed
 *   background task.
 * - ``_AsyncGuarantee`` -- asynchronous version. Use as
 *   ``await with_async(laila.guarantee_async, async () => { ... })``. Same
 *   idea, but with ``await`` instead of ``wait``, plus a background watcher
 *   task that *immediately* cancels the parent task if any in-scope future
 *   errors -- so a hanging coroutine does not have to wait for ``__aexit__``
 *   to learn about a failure.
 *
 * Both classes co-operate with ``_LAILA_IDENTIFIABLE_CENTRAL_COMMAND``'s
 * ``_guarantee_stack`` / ``_guarantee_enter`` / ``_guarantee_exit`` helpers,
 * which keep a thread-local stack of "active scopes" and register every
 * newly-submitted future with whichever scope is on top.
 *
 * Module-level singletons ``guarantee`` and ``guarantee_async`` are the
 * user-facing handles -- the classes themselves are private because there's
 * no point in instantiating more than one.
 */
import * as asyncio from "../_compat/asyncio.js";
import { CancelledError } from "../_compat/errors.js";
import { lazy } from "../_compat/lazy.js";
import { dict_items, hasattr } from "../_compat/pytypes.js";

/**
 * Synchronous context manager that waits for futures created in its scope.
 *
 * Pushes a new guarantee scope on the active policy's command stack on entry;
 * on exit, blocks until every future registered while the scope was active
 * has terminated. If any of those futures errored, the first observed
 * exception is re-raised after the wait completes -- but only if the body of
 * the ``with`` block did not itself raise (in which case the original
 * exception propagates).
 *
 * Use as ``with_(laila.guarantee, () => ...)``. Multiple nested guarantee
 * blocks compose: each scope only waits for the futures created *between its
 * own __enter__ and __exit__*.
 */
export class _Guarantee {
  /**
   * Push a fresh frame onto the active policy's guarantee stack.
   *
   * Newly-submitted futures will register with this frame until ``__exit__``
   * pops it.
   */
  __enter__() {
    const laila = lazy("laila");
    laila.get_active_policy().central.command._guarantee_enter();
    return this;
  }

  /**
   * Pop the frame and synchronously wait for every registered future.
   *
   * Returns ``false`` so the caller's exception (if any) continues to
   * propagate. When the body did not raise but one of the in-scope futures
   * did, the first wait-error is raised instead.
   */
  __exit__(exc_type, _exc, _tb) {
    const laila = lazy("laila");
    const created_inside = laila.get_active_policy().central.command._guarantee_exit();

    const wait_errors = [];
    for (const future of created_inside) {
      try {
        future.wait(null);
      } catch (wait_exc) {
        wait_errors.push(wait_exc);
      }
    }

    if (exc_type !== null && exc_type !== undefined) return false;

    if (wait_errors.length) throw wait_errors[0];

    return false;
  }

  enter() {
    return this.__enter__();
  }
  exit(...a) {
    return this.__exit__(...a);
  }
  [Symbol.dispose]() {
    this.__exit__(null, null, null);
  }
}

/**
 * Per-entry state for one open ``async with guarantee_async`` frame.
 *
 * A fresh instance is created on every ``_AsyncGuarantee.__aenter__`` and
 * discarded on the matching ``_AsyncGuarantee.__aexit__``. Keeping state here
 * (rather than on the singleton ``_AsyncGuarantee``) is what makes
 * ``guarantee_async`` safely reentrant: nested or concurrent ``async with``
 * blocks each get their own watcher task and their own background-exception
 * slot, so the inner frame can no longer clobber the outer frame's watcher
 * reference.
 */
class _AsyncGuaranteeScope {
  constructor() {
    this.command = null;
    this.scope = null;
    this.parent_task = null;
    this.watcher_task = null;
    this.background_exception = null;
  }
}

/**
 * Asynchronous context manager that awaits futures created in its scope.
 *
 * Same lexical guarantee as ``_Guarantee``, but for coroutine code:
 *
 * - Inside the ``async with`` block, a *background watcher* task polls the
 *   in-scope future list and immediately cancels the parent task on the
 *   first observed exception. This keeps a hung coroutine from sitting
 *   forever on a dead future. (A JS coroutine cannot be interrupted; the
 *   cancellation settles the parent ``Task`` for everyone awaiting it and is
 *   visible to the body through ``current_task().cancelling()``.)
 * - On exit, every still-pending in-scope future is awaited (proper ``await``
 *   for awaitable futures, ``asyncio.to_thread`` for sync-only ones), then --
 *   if no original exception -- the first wait-error or background-error is
 *   re-raised.
 *
 * Use as ``await with_async(laila.guarantee_async, async () => ...)``. Like
 * ``_Guarantee``, multiple nested scopes compose cleanly: each entry
 * allocates its own ``_AsyncGuaranteeScope``, kept on a per-task stack so
 * concurrent ``async with`` blocks from different tasks never share state.
 */
export class _AsyncGuarantee {
  /**
   * Initialise the per-task stack used to make the singleton reentrant.
   *
   * All real per-scope state lives on ``_AsyncGuaranteeScope`` instances;
   * this map only tracks which scope belongs to which task so ``__aexit__``
   * can pop the right one.
   */
  constructor() {
    /** @type {Map<any, _AsyncGuaranteeScope[]>} */
    this._task_stacks = new Map();
  }

  /** Enter the async guarantee scope and start the background watcher. */
  async __aenter__() {
    const laila = lazy("laila");
    const state = new _AsyncGuaranteeScope();
    state.command = laila.get_active_policy().central.command;
    state.command._guarantee_enter();
    const stack = state.command._guarantee_stack();
    state.scope = stack[stack.length - 1];
    state.parent_task = asyncio.current_task();
    state.watcher_task = asyncio.create_task(() => this._watch_for_future_errors(state));

    if (!this._task_stacks.has(state.parent_task)) this._task_stacks.set(state.parent_task, []);
    this._task_stacks.get(state.parent_task).push(state);
    return this;
  }

  /** Exit the scope, cancel the watcher, and await all enclosed futures. */
  async __aexit__(exc_type, _exc, _tb) {
    const task = asyncio.current_task();
    const stack = this._task_stacks.get(task);
    if (!stack || stack.length === 0) return false;
    const state = stack.pop();
    if (stack.length === 0) this._task_stacks.delete(task);

    const created_inside = state.command._guarantee_exit();

    if (state.watcher_task !== null && !state.watcher_task.done()) {
      state.watcher_task.cancel();
      try {
        await state.watcher_task;
      } catch (e) {
        if (!(e instanceof CancelledError)) throw e;
      }
    }

    // If the watcher saw an in-scope future error it cancelled our parent
    // task (see ``_watch_for_future_errors``). Clear that pending
    // cancellation so the original exception surfaces from
    // ``background_exception`` instead.
    if (state.background_exception !== null) {
      const current = asyncio.current_task();
      if (current !== null && hasattr(current, "uncancel")) current.uncancel();
    }

    const wait_errors = [];
    for (const future of created_inside) {
      try {
        if (hasattr(future, "__await__") || typeof future.then === "function") await future;
        else await asyncio.to_thread(() => future.wait(null));
      } catch (wait_exc) {
        wait_errors.push(wait_exc);
      }
    }

    if (state.background_exception !== null) throw state.background_exception;

    if (exc_type !== null && exc_type !== undefined) return false;

    if (wait_errors.length) throw wait_errors[0];

    return false;
  }

  /** Poll for newly registered futures and cancel the parent on error. */
  async _watch_for_future_errors(state) {
    /** @type {Map<string, asyncio.Task>} */
    const watched = new Map();
    const me = asyncio.current_task();

    while (true) {
      if (me !== null && me.cancelling() > 0) throw new CancelledError();

      for (const [future_id, future] of dict_items(state.scope)) {
        if (watched.has(future_id)) continue;
        watched.set(future_id, asyncio.create_task(() => this._await_future(future)));
      }

      if (watched.size === 0) {
        await asyncio.sleep(0.01);
        continue;
      }

      const [done] = await asyncio.wait(watched.values(), { timeout: 0.01, return_when: asyncio.FIRST_COMPLETED });

      for (const [future_id, task] of [...watched.entries()]) {
        if (!done.has(task)) continue;
        watched.delete(future_id);
        try {
          await task;
        } catch (exc) {
          state.background_exception = exc;
          if (state.parent_task !== null && !state.parent_task.done()) state.parent_task.cancel();
          return;
        }
      }
    }
  }

  /** Await a single future, adapting sync futures via ``asyncio.to_thread``. */
  async _await_future(future) {
    if (hasattr(future, "__await__") || typeof future.then === "function") return await future;
    return await asyncio.to_thread(() => future.wait(null));
  }

  enter() {
    return this.__aenter__();
  }
  exit(...a) {
    return this.__aexit__(...a);
  }
  async [Symbol.asyncDispose]() {
    await this.__aexit__(null, null, null);
  }
}

export const guarantee = new _Guarantee();
export const guarantee_async = new _AsyncGuarantee();
