/**
 * Synchronous event-loop pump (the JS stand-in for a Python thread blocking
 * on `threading.Event.wait()` while other threads keep running).
 *
 * `pump_until(pred, timeout)` spins the current thread's libuv loop one
 * iteration at a time, draining nextTicks + microtasks between iterations,
 * until `pred()` is truthy or `timeout` seconds elapse. The caller's stack is
 * preserved; everything else in the process keeps making progress.
 *
 * Availability: requires the optional native addon (`native/loop_pump.c`) and
 * `process._tickCallback`. `LAILA_DISABLE_PUMP=1` forces the unavailable path
 * (used by tests of the fallback behaviour).
 */
import { createRequire } from "node:module";
import { fileURLToPath } from "node:url";
import { executionAsyncResource } from "node:async_hooks";
import path from "node:path";

const require = createRequire(import.meta.url);
const __dirname = path.dirname(fileURLToPath(import.meta.url));

let _native = null;
let _unavailable_reason = null;

function _load() {
  if (_native || _unavailable_reason) return;
  if (process.env.LAILA_DISABLE_PUMP === "1") {
    _unavailable_reason = "LAILA_DISABLE_PUMP=1";
    return;
  }
  if (typeof process._tickCallback !== "function") {
    _unavailable_reason = "process._tickCallback is unavailable on this Node build";
    return;
  }
  try {
    const load = require("node-gyp-build");
    const mod = load(path.join(__dirname, "..", "..", "native"));
    if ((mod.abi | 0) >= 2) _native = mod;
    else _unavailable_reason = "native loop pump addon is stale (rebuild: npm run build:native)";
  } catch (err) {
    _unavailable_reason = `native loop pump addon not built (${err && err.message})`;
  }
}

// ---------------------------------------------------------------------------
// Macrotask hops
// ---------------------------------------------------------------------------
//
// Everything in laila that Python would run "on another thread" (executor
// bodies, `Thread.start`, loop-thread hand-offs, `asyncio.sleep(0)`) is
// scheduled through `hop()` rather than `setImmediate()`:
//
// * Node's `processImmediate` is not re-entrant: a nested `uv_run` (the pump)
//   started from the 2nd+ immediate of a batch aborts the process. Hops are
//   delivered as `MessageChannel` messages instead -- Node dispatches those
//   from the loop's *poll* phase through its regular callback machinery
//   (uncaught-exception handling, nextTick/microtask drain), so a blocking
//   wait inside a hop never sits inside `processImmediate`.
// * They keep running inside nested pumps even while Node's own immediates
//   are suspended (see `pump_until`).
//
// Semantics match `setImmediate`: FIFO, one batch per loop iteration (hops
// queued while draining run on the next iteration), a throwing hop surfaces
// as `uncaughtException` and does not stop the others. The channel is
// referenced only while a batch is pending, so it never keeps the process
// alive on its own.
//
// Interleaving. A hop that *blocks* (a sync body in `Event.wait()`, a
// `Thread` waiting on a future) pumps the loop underneath itself, and the
// rest of its batch is dispatched from that nested pump -- the way Python's
// blocked threads let the others run -- instead of starving until it
// returns. The one exception is a frame that blocks while *holding a lock*
// (`with pool.atomic(): block_on(io)`): the rest of its batch (hops queued
// before the blocking hop was dispatched, `seq < _locked_floor`) stays
// queued until it returns, because a nested hop contending for that lock
// could never be satisfied -- the holder sits below it on the stack -- and
// would deadlock. Hops queued since (by the hop itself or during its wait)
// still run nested (the wait may depend on them), and once one is
// dispatched the queue drains in FIFO order as it always has, deferred hops
// included.
const _hops = [];
let _hop_seq = 0;
/** ``_hop_seq`` when the innermost running hop was dispatched. */
let _dispatch_floor = 0;
let _hop_scheduled = false;
/** Pump frames currently on the stack: ``{floor, locked}``. */
const _pump_frames = [];
/** Highest ``floor`` among lock-holding pump frames (-1 when none). */
let _locked_floor = -1;

function _recompute_locked_floor() {
  let floor = -1;
  for (const f of _pump_frames) if (f.locked && f.floor > floor) floor = f.floor;
  _locked_floor = floor;
}

function _has_runnable_hop() {
  for (const h of _hops) if (h.seq >= _locked_floor) return true;
  return false;
}
let _hop_port_rx = null;
let _hop_port_tx = null;

function _hop_channel() {
  if (_hop_port_tx === null) {
    const { port1, port2 } = new MessageChannel();
    port1.on("message", _run_hops);
    port1.unref();
    port2.unref();
    _hop_port_rx = port1;
    _hop_port_tx = port2;
  }
  return _hop_port_tx;
}

function _schedule_hops() {
  _hop_scheduled = true;
  const tx = _hop_channel();
  _hop_port_rx.ref();
  tx.postMessage(null);
}

/**
 * Schedule `fn` on the next macrotask (laila's `setImmediate`).
 * @param {() => void} fn
 */
export function hop(fn) {
  _hops.push({ fn, seq: _hop_seq++ });
  if (!_hop_scheduled) _schedule_hops();
}

function _run_hops() {
  _hop_scheduled = false;
  _hop_port_rx.unref();
  try {
    // Everything queued predates a lock-holding wait below us: leave it for
    // when that wait returns (see the interleaving notes above).
    if (!_has_runnable_hop()) return;
    // Only what is queued now; later arrivals are scheduled by `hop()`.
    let n = _hops.length;
    while (n-- > 0 && _hops.length) {
      const item = _hops.shift();
      // Post the next message *before* running the hop so that, should it
      // block, the nested pump dispatches the rest of the batch. If it does
      // not block the extra message finds nothing runnable and is a no-op.
      if (!_hop_scheduled && _has_runnable_hop()) _schedule_hops();
      const outer_floor = _dispatch_floor;
      _dispatch_floor = _hop_seq;
      try {
        item.fn();
      } catch (err) {
        process.nextTick(() => {
          throw err;
        });
      } finally {
        _dispatch_floor = outer_floor;
      }
    }
  } finally {
    if (!_hop_scheduled && _has_runnable_hop()) _schedule_hops();
  }
}

// ---------------------------------------------------------------------------
// Microtask draining
// ---------------------------------------------------------------------------
//
// Node drains nextTicks + microtasks when the *outermost* native callback
// scope closes. Inside a pump that scope is still open, so the pump drains
// explicitly with `process._tickCallback()` (as `deasync` does). One hazard:
// Node assumes that function is only ever called from its own scope-close
// path, where a throwing nextTick callback (`stream.emit('error')` without a
// listener, the `process.nextTick(() => { throw err })` idiom, ...) becomes an
// uncaught exception whose handler also resets the async-hooks id stack.
// Called from JS, the same throw would (a) surface as if `Future.wait()` had
// thrown and (b) leave that stack unbalanced, which Node treats as fatal at
// the next scope close. `_drain_ticks` catches it, marks the stack dirty and
// re-throws from a timer -- Node's standard path -- after which the stack is
// clean again; until then the pump only spins the loop.
let _ticks_dirty = false;

function _drain_ticks() {
  if (_ticks_dirty) return;
  try {
    process._tickCallback();
  } catch (err) {
    _ticks_dirty = true;
    setTimeout(() => {
      _ticks_dirty = false;
      throw err;
    }, 0);
  }
}

/**
 * True when the current frame is executing inside a Node `Immediate`
 * callback (`setImmediate`). A pump started there must keep Node's immediate
 * processing from re-entering (see `pump_until`).
 */
function _inside_node_immediate() {
  const r = executionAsyncResource();
  return r !== null && r !== undefined && typeof r === "object" && r.constructor !== undefined && r.constructor.name === "Immediate";
}

/* Number of active pumps that must keep Node's check/idle handles suspended. */
let _suspend_depth = 0;

/** @returns {boolean} true when synchronous blocking waits are possible. */
export function pump_available() {
  _load();
  return _native !== null;
}

/** @returns {string|null} why the pump is unavailable, or null when it is. */
export function pump_unavailable_reason() {
  _load();
  return _unavailable_reason;
}

/* Nested-pump depth guard: nested `wait()` -> pump -> callback -> `wait()`
 * stacks native frames. Parking removes deadlocks but not stack growth. */
let _depth = 0;
const _MAX_DEPTH = 200;

/** Current nesting depth of active pumps (0 when no synchronous wait is in progress). */
export function pump_depth() {
  return _depth;
}

function _noop() {}

/**
 * True when the caller is executing *inside* a V8 microtask job (code after an
 * `await`, a `.then` callback, or the top level of an ES module, which Node
 * evaluates from a promise job). V8 refuses re-entrant microtask checkpoints,
 * so no promise can settle until that job returns: a synchronous wait there
 * can never complete. This is the JS analogue of Python's "called from a loop
 * thread" condition. Detected with a probe microtask + explicit drain.
 *
 * Safe contexts: CommonJS top level, timers, I/O callbacks, `setImmediate`,
 * `process.nextTick`, and anything they call synchronously.
 */
export function in_microtask() {
  if (typeof process._tickCallback !== "function") return false;
  let ran = false;
  queueMicrotask(() => {
    ran = true;
  });
  _drain_ticks();
  return !ran;
}

/**
 * Block the current stack until `pred()` is truthy or `timeout` (seconds, `null`
 * = forever) elapses.
 *
 * @param {() => boolean} pred
 * @param {number|null} [timeout]
 * @param {boolean} [locked] the waiting frame holds a lock (see the hop
 *   interleaving notes above)
 * @returns {boolean} true if `pred()` became truthy, false on timeout.
 * @throws {Error} when the pump is unavailable or the caller is inside a
 *   microtask (callers translate both into `LoopBlockingWaitError`).
 */
export function pump_until(pred, timeout = null, locked = false) {
  if (pred()) return true;
  _load();
  if (!_native) {
    const err = new Error(`synchronous wait is not possible: ${_unavailable_reason}`);
    err.code = "ERR_LAILA_PUMP_UNAVAILABLE";
    throw err;
  }
  if (in_microtask()) {
    const err = new Error(
      "synchronous wait is not possible inside a microtask (after `await`, in a " +
        "`.then` callback, or at the top level of an ES module): no promise can " +
        "settle until the current job returns. Use `await`, or call from a " +
        "CommonJS script / timer / setImmediate callback.",
    );
    err.code = "ERR_LAILA_PUMP_IN_MICROTASK";
    throw err;
  }
  if (pred()) return true;
  if (_depth >= _MAX_DEPTH) {
    const err = new Error(`nested synchronous waits exceeded ${_MAX_DEPTH} levels`);
    err.code = "ERR_LAILA_PUMP_DEPTH";
    throw err;
  }
  const has_deadline = timeout !== null && timeout !== undefined;
  const deadline = has_deadline ? performance.now() + Math.max(0, timeout) * 1000 : Infinity;
  // A *referenced* timer at the deadline guarantees uv_run(UV_RUN_ONCE) wakes
  // up in time even when the only other handles are idle sockets/servers. The
  // timer also flags expiry itself: libuv's clock is ms-granular and may fire
  // a fraction of a ms before `performance.now()` reaches the deadline, and a
  // further `run_once` would then block on the idle handles indefinitely.
  let expired = false;
  let deadline_timer = null;
  if (has_deadline && Number.isFinite(deadline)) {
    deadline_timer = setTimeout(() => {
      expired = true;
    }, Math.max(0, deadline - performance.now()));
  }
  const timed_out = () => expired || performance.now() >= deadline;
  // Started from inside a Node `Immediate` callback (directly, or nested in a
  // pump that was): Node's `processImmediate` is on the stack and must not be
  // re-entered, so its check/idle handles are suspended for every iteration
  // of this pump. Node's own immediates are deferred until it unwinds; laila's
  // hops are unaffected (they run from the addon's async handle).
  const suspend = _suspend_depth > 0 || _inside_node_immediate();
  _depth += 1;
  if (suspend) _suspend_depth += 1;
  // Hops queued since the running hop was dispatched (by it, or by the
  // callbacks of an enclosing pump) count as "during" this wait.
  const frame = { floor: _dispatch_floor, locked: !!locked };
  _pump_frames.push(frame);
  if (frame.locked && frame.floor > _locked_floor) _locked_floor = frame.floor;
  try {
    for (;;) {
      _drain_ticks();
      if (!_ticks_dirty) {
        if (pred()) return true;
        if (timed_out()) return false;
      }
      const alive = _native.run_once(suspend);
      _drain_ticks();
      if (_ticks_dirty) continue; // let the re-thrown tick error land first
      if (pred()) return true;
      if (timed_out()) return false;
      if (!alive && !_hop_scheduled) {
        // Nothing referenced is left in the loop, so uv_run returns at once
        // without polling -- and a "dead" loop never runs *unref'd* handles
        // either. Node delivers V8 platform tasks (``WebAssembly.instantiate``
        // completions, ``Atomics.waitAsync`` wake-ups, ...) through an unref'd
        // uv_async, so a wait that depends on one would spin forever. Arm a
        // short referenced timer: the next iteration then polls (<= 5 ms),
        // dispatching whatever is pending, instead of busy-sleeping.
        setTimeout(_noop, 5);
      }
    }
  } finally {
    _depth -= 1;
    if (suspend) _suspend_depth -= 1;
    if (deadline_timer) clearTimeout(deadline_timer);
    _pump_frames.splice(_pump_frames.lastIndexOf(frame), 1);
    if (frame.locked) {
      _recompute_locked_floor();
      // Hops deferred behind this lock-holding wait become runnable.
      if (!_hop_scheduled && _has_runnable_hop()) _schedule_hops();
    }
  }
}
