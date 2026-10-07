/**
 * Slot parking -- release a taskforce slot while awaiting another future.
 *
 * Problem
 * -------
 * A taskforce has a bounded number of execution *slots* (for the default
 * ``PythonAsyncThreadPoolTaskForce`` that is ``num_workers *
 * max_async_per_thread``). A task that holds a slot and then blocks on a
 * *child* future that itself needs a slot on the same taskforce creates a
 * hold-and-wait dependency. When every slot is held by such a parent, no
 * child can ever be dispatched: a classic deadlock. Nesting is pervasive
 * in laila -- ``laila.remember`` submits per-entry fetches, constitution
 * bodies call ``manifest.realized`` which remembers children, and users
 * legitimately ``await laila.remember(...)`` from inside their own
 * submitted coroutines -- so this must be solved once, in the scheduler,
 * not by asking every caller to reason about slot budgets.
 *
 * Solution
 * --------
 * Every runner coroutine publishes a ``_SlotCtx`` in the ``_CURRENT_SLOT``
 * context variable. Whenever code that is running *inside* a slot waits on
 * a laila future (``await fut``, ``fut.wait()``, ``fut.result``/``fut.data``),
 * the wait is wrapped in ``park_async`` or ``park_sync``. Those helpers
 *
 * 1. hand the slot back to the loop thread (``_park_enter``), so the
 *    dispatcher -- or a previously parked sibling -- can use it, and
 * 2. re-acquire a slot before returning to the caller (``_park_exit_*``),
 *    so the invariant "at most ``cap`` coroutines *run* per loop thread"
 *    still holds for the CPU/IO work between waits.
 *
 * While parked, the coroutine is suspended at an ``await`` and consumes no
 * slot; only the parent's stack frame (memory) is retained. Because a
 * parked task can never hold a slot while it waits for a child, the
 * wait-for graph has no slot cycle and progress is guaranteed as long as
 * the leaf tasks terminate.
 *
 * Re-acquisition is *prioritised*: a loop thread hands a freed slot to a
 * waiting parked task before decrementing its in-flight count, so parked
 * parents resume ahead of newly dispatched roots. This bounds the number of
 * simultaneously live trees instead of letting the dispatcher keep opening
 * new ones.
 *
 * Nesting and threads
 * -------------------
 * Parking is reentrant per slot. A depth counter in ``_SlotCtx`` ensures
 * that a wait nested inside another wait (e.g. ``GroupFuture.wait`` calling
 * each child's ``wait``) releases the slot exactly once and re-acquires it
 * exactly once, at the outermost wait. The context is propagated through
 * ``asyncio.to_thread`` / ``run_in_executor`` (``contextvars.copy_context``),
 * so a *synchronous* constitution body running on an executor thread that
 * calls ``fut.wait()`` parks the slot of the coroutine that offloaded it.
 *
 * Compute permits
 * ---------------
 * Sync bodies are offloaded to an executor thread. The taskforce bounds the
 * number of *executing* sync bodies with a semaphore of ``sync_workers``
 * permits rather than bounding the number of threads. ``_CURRENT_PERMIT``
 * carries the permit held by the current executor thread; ``park_sync``
 * releases it while the body blocks on a laila future and re-acquires it
 * afterwards. A body that is blocked therefore consumes neither a slot nor
 * a permit, which is what makes arbitrarily deep sync nesting safe.
 *
 * Cycle guard
 * -----------
 * ``_RESOLVE_CHAIN`` carries the tuple of entry global-ids that are
 * currently being resolved (remembered or built) along the *causal* chain
 * of nested submissions. It is snapshotted at submit time and re-installed
 * by the runner, so a child root task inherits its parent's chain even
 * though it runs on a different loop thread. ``laila.remember`` and
 * ``laila.build`` consult it to raise ``CyclicDependencyError`` instead of
 * recursing forever when a constitution ends up depending on itself.
 */
import { RuntimeError } from "../../../../_compat/errors.js";
import { ContextVar } from "../../../../_compat/contextvars.js";
import { register } from "../../../../_compat/lazy.js";
import { Lock, Event, with_lock } from "../../../../_compat/threading.js";
import { pump_depth } from "../../../../_compat/pump.js";
import * as asyncio from "../../../../_compat/asyncio.js";

/**
 * Raised when an entry resolution depends (transitively) on itself.
 *
 * Detected via ``_RESOLVE_CHAIN``: if ``laila.remember`` or ``laila.build``
 * is invoked for a global-id that is already on the current chain of nested
 * resolutions, the dependency graph has a cycle and no amount of scheduling
 * can complete it.
 */
export class CyclicDependencyError extends RuntimeError {}

/**
 * Bookkeeping for one occupied taskforce slot.
 *
 * - ``tf``: the owning taskforce (duck-typed: needs ``_capacity_cv`` and ``_stop``).
 * - ``lt``: the loop thread whose slot this context represents (duck-typed:
 *   needs ``release_slot``, ``try_reserve_or_wait``, ``cancel_wait``).
 * - ``depth``: number of nested parks currently active. Only the transition
 *   ``0 -> 1`` releases the slot and only ``1 -> 0`` re-acquires it.
 * - ``holds_slot``: whether this context currently owns a slot on ``lt``.
 * - ``lock``: guards ``depth`` and ``holds_slot``.
 */
export class _SlotCtx {
  constructor(tf, lt) {
    this.tf = tf;
    this.lt = lt;
    this.depth = 0;
    this.holds_slot = true;
    this.lock = new Lock();
  }
}

/**
 * Slot context of the runner coroutine the current code is executing under.
 *
 * ``null`` when the caller is not inside any taskforce slot (main thread,
 * plain user threads, the dispatcher). Propagates into executor threads via
 * ``contextvars.copy_context`` so sync bodies can park too.
 * @type {ContextVar<_SlotCtx|null>}
 */
export const _CURRENT_SLOT = new ContextVar("_laila_current_slot", { default: null });

/**
 * Tuple of entry global-ids being resolved along the current causal chain.
 * @type {ContextVar<readonly string[]>}
 */
export const _RESOLVE_CHAIN = new ContextVar("_laila_resolve_chain", { default: Object.freeze([]) });

/**
 * A compute permit held by a sync body running on an executor thread.
 *
 * ``sem`` is the taskforce-wide semaphore bounding concurrently *executing*
 * sync bodies; ``held`` tracks whether this thread currently owns one of its
 * permits (released while parked).
 */
export class _Permit {
  constructor(sem) {
    this.sem = sem;
    this.held = false;
  }
  acquire() {
    if (!this.held) {
      this.sem.acquire();
      this.held = true;
    }
  }
  /** Coroutine-friendly acquire (Node has no real blocking executor threads). */
  async acquire_async() {
    if (!this.held) {
      await this.sem.acquire_async();
      this.held = true;
    }
  }
  release() {
    if (this.held) {
      this.held = false;
      this.sem.release();
    }
  }
}

/**
 * Compute permit of the executor thread the current sync body runs on.
 * @type {ContextVar<_Permit|null>}
 */
export const _CURRENT_PERMIT = new ContextVar("_laila_current_permit", { default: null });

// ---------------------------------------------------------------------------
// release / re-acquire primitives
// ---------------------------------------------------------------------------
function _notify_capacity(tf) {
  const cv = tf ? tf._capacity_cv : undefined;
  if (cv == null) return;
  cv.acquire();
  try {
    cv.notify();
  } finally {
    cv.release();
  }
}

/** Increment park depth; on the outermost park hand the slot back. */
export function _park_enter(ctx) {
  const released = with_lock(ctx.lock, () => {
    ctx.depth += 1;
    if (ctx.depth > 1 || !ctx.holds_slot) return false;
    ctx.holds_slot = false;
    return true;
  });
  if (!released) return;
  if (ctx.lt.release_slot()) _notify_capacity(ctx.tf);
}

function _stop_requested(ctx) {
  const stop = ctx.tf ? ctx.tf._stop : undefined;
  return stop != null && stop.is_set();
}

/**
 * Decrement park depth; on the outermost exit re-acquire a slot.
 *
 * If the taskforce is shutting down the re-acquire is skipped: the task
 * will be cancelled by the loop teardown anyway and must not block on a
 * slot that will never be handed out.
 */
export async function _park_exit_async(ctx) {
  const done = with_lock(ctx.lock, () => {
    ctx.depth -= 1;
    return ctx.depth > 0 || ctx.holds_slot;
  });
  if (done) return;
  if (_stop_requested(ctx)) return;
  const fut = new asyncio.Future();
  const _wake = () => {
    if (!fut.done()) fut.set_result(null);
  };
  const cb = () => {
    try {
      queueMicrotask(_wake);
    } catch {
      // Loop closed during shutdown; nothing to resume.
    }
  };
  if (ctx.lt.try_reserve_or_wait(cb)) {
    with_lock(ctx.lock, () => {
      ctx.holds_slot = true;
    });
    return;
  }
  try {
    await fut;
  } catch (err) {
    // Cancelled while waiting. If the slot was already handed to us we own
    // it and the runner's finally-block must release it.
    if (!ctx.lt.cancel_wait(cb)) {
      with_lock(ctx.lock, () => {
        ctx.holds_slot = true;
      });
    }
    throw err;
  }
  with_lock(ctx.lock, () => {
    ctx.holds_slot = true;
  });
}

/** Sync counterpart of ``_park_exit_async`` for executor threads. */
export function _park_exit_sync(ctx) {
  const done = with_lock(ctx.lock, () => {
    ctx.depth -= 1;
    return ctx.depth > 0 || ctx.holds_slot;
  });
  if (done) return;
  if (_stop_requested(ctx)) return;
  const ev = new Event();
  const cb = () => ev.set();
  // Node: this waiter blocks by pumping the loop underneath itself, so it
  // can only resume once every deeper sync wait has returned. It is tagged
  // with its depth so the loop thread can (a) prefer the deepest waiter and
  // (b) take a slot back from a waiter that got buried before it could
  // resume (``_LoopThread.try_reserve_or_wait`` / ``release_slot``).
  cb._sync_depth = pump_depth();
  cb._revoke = () => ev.clear();
  if (ctx.lt.try_reserve_or_wait(cb)) {
    with_lock(ctx.lock, () => {
      ctx.holds_slot = true;
    });
    return;
  }
  while (!ev.wait(0.05)) {
    if (_stop_requested(ctx)) {
      if (ctx.lt.cancel_wait(cb)) return;
      break;
    }
  }
  ctx.lt.waiter_resumed(cb);
  with_lock(ctx.lock, () => {
    ctx.holds_slot = true;
  });
}

// ---------------------------------------------------------------------------
// public helpers
// ---------------------------------------------------------------------------
/**
 * Await *awaitable*, parking the current slot (if any) for the duration.
 *
 * Outside a taskforce slot this is a plain ``await``.
 */
export async function park_async(awaitable) {
  const ctx = _CURRENT_SLOT.get();
  if (ctx === null) return await awaitable;
  _park_enter(ctx);
  try {
    return await awaitable;
  } finally {
    await _park_exit_async(ctx);
  }
}

/**
 * Call ``fn(...args)``, parking the current slot (if any).
 *
 * Intended for blocking waits executed on executor threads that were
 * spawned from inside a slot (``asyncio.to_thread`` copies the context, so
 * ``_CURRENT_SLOT`` is visible there). Outside a slot this is a plain call.
 */
export function park_sync(fn, ...args) {
  const ctx = _CURRENT_SLOT.get();
  const permit = _CURRENT_PERMIT.get();
  if (ctx === null && permit === null) return fn(...args);
  // Give up the compute permit first (never hold a permit while waiting),
  // then the slot.
  const released_permit = permit !== null && permit.held;
  if (released_permit) permit.release();
  if (ctx !== null) _park_enter(ctx);
  try {
    return fn(...args);
  } finally {
    // Re-acquire in the opposite order: slot, then permit. A thread that
    // holds a slot and waits for a permit cannot deadlock because every
    // permit holder is executing (and will finish or park).
    if (ctx !== null) _park_exit_sync(ctx);
    if (released_permit) permit.acquire();
  }
}

/** Raise ``CyclicDependencyError`` if any id is already being resolved. */
export function check_resolve_cycle(...global_ids) {
  const chain = _RESOLVE_CHAIN.get();
  if (!chain || chain.length === 0) return;
  for (const gid of global_ids) {
    if (chain.includes(gid)) {
      throw new CyclicDependencyError(
        `Cyclic dependency: ${gid} is already being resolved on the ` + `current chain (${chain.join(" -> ")} -> ${gid}).`,
      );
    }
  }
}

/** Return the current chain extended with *global_ids* (no mutation). */
export function resolve_chain_with(...global_ids) {
  return Object.freeze([..._RESOLVE_CHAIN.get(), ...global_ids]);
}

register("laila.policy.central.command.schema.parking", {
  _CURRENT_PERMIT,
  _CURRENT_SLOT,
  _RESOLVE_CHAIN,
  CyclicDependencyError,
  _Permit,
  _SlotCtx,
  check_resolve_cycle,
  park_async,
  park_sync,
  resolve_chain_with,
});
