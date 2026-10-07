/**
 * Port of ``tests/deep_eval/test_09_command_futures_taskforce_deep.py``.
 *
 * Deep tests for the Central Command, task-forces, slot parking, futures,
 * group futures, callbacks, nested submission guards and ``laila.guarantee``.
 */
import { test, describe } from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { S, laila, macrotask, run_async as run, with_fresh_policy_async as with_fresh_policy } from "./_fixtures.js";

const E = await import(S + "_compat/errors.js");
const T = await import(S + "_compat/pytypes.js");
const TH = await import(S + "_compat/threading.js");
const asyncio = await import(S + "_compat/asyncio.js");
const time = await import(S + "_compat/time.js");
const { partial } = await import(S + "_compat/functools.js");
const { with_, with_async } = await import(S + "_compat/contextlib.js");
const { repr } = await import(S + "_compat/pyrepr.js");
const { Entry } = await import(S + "entry/entry.js");
const { _LAILA_IDENTIFIABLE_CENTRAL_COMMAND } = await import(S + "policy/central/command/schema/base.js");
const EXC = await import(S + "policy/central/command/schema/exceptions.js");
const {
  LoopBlockingWaitError,
  NestedCommandSubmitError,
  _check_no_pending_submit_owner,
  _check_not_loop_thread,
  _register_async_loop_thread,
  _unregister_async_loop_thread,
  ensure_coroutine_function,
  no_command_submit,
} = EXC;
const { Future } = await import(S + "policy/central/command/schema/future/future/future.js");
const { _LAILA_IDENTIFIABLE_FUTURE } = await import(S + "policy/central/command/schema/future/future/future_identity.js");
const { FutureStatus } = await import(S + "policy/central/command/schema/future/future/future_status.js");
const { GroupFuture } = await import(S + "policy/central/command/schema/future/future/group_future.js");
const PK = await import(S + "policy/central/command/schema/parking.js");
const { _RESOLVE_CHAIN, CyclicDependencyError, check_resolve_cycle, park_async, park_sync, resolve_chain_with } = PK;
const { PythonAsyncThreadPoolTaskForce } = await import(S + "policy/central/command/taskforce/async_thread_pool_executor/taskforce.js");
const { _LAILA_IDENTIFIABLE_TASK_FORCE, _live_taskforces_snapshot } = await import(S + "policy/central/command/taskforce/base.js");
const { TaskForceStatus } = await import(S + "policy/central/command/taskforce/status.js");
const { ConcurrentPackageFuture } = await import(S + "policy/central/command/taskforce/thread_pool_executor/future.js");
const { Manifest } = await import(S + "policy/central/memory/schema/manifest.js");
const { _LAILA_IDENTIFIABLE_POLICY } = await import(S + "policy/schema/base.js");

const { NotImplemented, dict_has, dict_get, dict_len, dict_values, getitem } = T;

const WAIT = 10;

const TMP_ROOT = fs.mkdtempSync(path.join(os.tmpdir(), "laila_deep09_"));
laila.set_default_directory(TMP_ROOT);

const range = (n) => Array.from({ length: n }, (_, i) => i);
const sorted = (xs) => [...xs].sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));
const sum = (xs) => xs.reduce((a, b) => a + Number(b), 0);
const approx = (a, b) => assert.ok(Math.abs(Number(a) - Number(b)) < 1e-6, `${a} !~ ${b}`);
const pct_sum = (status) => sum(Object.values(status.percentages));

function _boom() {
  throw new E.ValueError("boom");
}

async function _aboom() {
  throw new E.KeyError("aboom");
}

/** Run *teardown* after *result* (sync value or promise) settles. */
function _finally(result, teardown) {
  if (result !== null && result !== undefined && typeof result.then === "function") {
    return result.then(
      (v) => {
        teardown();
        return v;
      },
      (e) => {
        teardown();
        throw e;
      },
    );
  }
  teardown();
  return result;
}

/** ``fresh_policy`` fixture. */
const tp = (name, fn) => test(name, () => with_fresh_policy(fn));
/** ``cmd`` fixture. */
const tc = (name, fn) => tp(name, (fresh_policy) => fn(fresh_policy.central.command, fresh_policy));
/** ``tf`` fixture: a private 1-worker taskforce registered with the fresh policy's command. */
const ttf = (name, fn) =>
  tp(name, (fresh_policy) => {
    const tf = new PythonAsyncThreadPoolTaskForce({ num_workers: 1, max_async_per_thread: 4, sync_workers: 2, policy_id: fresh_policy.global_id });
    fresh_policy.central.command.add_taskforce(tf);
    let out;
    try {
      out = fn(fresh_policy.central.command, tf, fresh_policy);
    } catch (e) {
      tf.shutdown({ wait: true, cancel_pending: true });
      throw e;
    }
    return _finally(out, () => tf.shutdown({ wait: true, cancel_pending: true }));
  });

/**
 * Python ``lambda: ev.wait(WAIT)`` -- a task that blocks until released.
 *
 * JS runs sync bodies on the one real thread: a blocking ``Event.wait`` in a
 * body pumps *underneath* the caller's own ``wait(0.05)`` / ``sleep``, which
 * can then only return once the body does, so the caller could never observe
 * the task in flight. The non-trapping equivalent is a coroutine awaiting the
 * event; the future / status / timeout semantics asserted are unchanged.
 */
const _blocks_on =
  (ev, value = undefined) =>
  async () => {
    const r = await ev.wait_async(WAIT);
    return value === undefined ? r : value;
  };

/** ``threading.Barrier(n, timeout)`` (cooperative threads). */
class Barrier {
  constructor(parties, timeout = null) {
    this.parties = parties;
    this.timeout = timeout;
    this._count = 0;
    this._ev = new TH.Event();
  }
  wait() {
    this._count += 1;
    if (this._count >= this.parties) {
      this._ev.set();
      return 0;
    }
    if (!this._ev.wait(this.timeout)) throw new E.RuntimeError("BrokenBarrierError");
    return this.parties - this._count;
  }
}

// ---------------------------------------------------------------------------
// FutureStatus / TaskForceStatus enums
// ---------------------------------------------------------------------------

describe("TestEnums", () => {
  test("test_future_status_values", () => {
    assert.equal(FutureStatus.FINISHED.value, "finished");
    assert.equal(FutureStatus.ERROR.value, "error");
    assert.equal(FutureStatus.CANCELLED.value, "cancelled");
    assert.equal(FutureStatus.RUNNING.value, "running");
    assert.equal(FutureStatus.NOT_STARTED.value, "not_started");
  });

  test("test_future_status_is_str", () => {
    assert.ok(FutureStatus.FINISHED instanceof String);
    assert.equal(FutureStatus("finished"), FutureStatus.FINISHED);
  });

  test("test_taskforce_status_values", () => {
    const names = new Set([...TaskForceStatus].map((s) => s.name));
    for (const n of ["NOT_STARTED", "RUNNING", "PAUSED", "STOPPED"]) assert.ok(names.has(n), n);
    assert.ok(TaskForceStatus.RUNNING instanceof String);
  });
});

// ---------------------------------------------------------------------------
// Central command structure
// ---------------------------------------------------------------------------

describe("TestCentralCommand", () => {
  tc("test_alpha_and_internal_taskforce_registered", (cmd) => {
    assert.ok(dict_has(cmd.taskforces, cmd.alpha_taskforce));
    assert.ok(dict_has(cmd.taskforces, cmd.internal_taskforce));
    assert.notEqual(cmd.alpha_taskforce, cmd.internal_taskforce);
  });

  tc("test_ranks", (cmd) => {
    assert.equal(getitem(cmd.taskforces, cmd.alpha_taskforce).rank, 2);
    assert.equal(getitem(cmd.taskforces, cmd.internal_taskforce).rank, 1);
  });

  tc("test_is_identifiable", (cmd) => {
    assert.ok(cmd instanceof _LAILA_IDENTIFIABLE_CENTRAL_COMMAND);
    assert.ok(cmd.global_id.startsWith("LAILA:"));
  });

  tc("test_add_taskforce_registers_by_gid", (cmd, fresh_policy) => {
    const t = new PythonAsyncThreadPoolTaskForce({ num_workers: 1, policy_id: fresh_policy.global_id });
    try {
      cmd.add_taskforce(t);
      assert.equal(getitem(cmd.taskforces, t.global_id), t);
    } finally {
      t.shutdown();
    }
  });

  tc("test_add_taskforce_overwrites_same_gid", (cmd, fresh_policy) => {
    const t1 = new PythonAsyncThreadPoolTaskForce({ num_workers: 1, policy_id: fresh_policy.global_id, uuid: "11111111-1111-1111-1111-111111111111" });
    const t2 = new PythonAsyncThreadPoolTaskForce({ num_workers: 1, policy_id: fresh_policy.global_id, uuid: "11111111-1111-1111-1111-111111111111" });
    try {
      cmd.add_taskforce(t1);
      cmd.add_taskforce(t2);
      assert.equal(getitem(cmd.taskforces, t1.global_id), t2);
    } finally {
      t1.shutdown();
      t2.shutdown();
    }
  });

  tc("test_submit_unknown_taskforce_keyerror", (cmd) => {
    assert.throws(() => cmd.submit([() => 1], { taskforce_id: "LAILA:TASK_FORCE:00000000-0000-0000-0000-000000000000" }), E.KeyError);
  });

  tc("test_await_not_implemented", (cmd) => {
    assert.throws(() => cmd.__await__(), E.NotImplementedError);
  });

  tp("test_laila_command_resolves_to_active_policy", (fresh_policy) => {
    assert.equal(laila.command, fresh_policy.central.command);
  });

  tp("test_shutdown_stops_all_taskforces", (fresh_policy) => {
    const cmd = fresh_policy.central.command;
    cmd.shutdown({ wait: true, cancel_pending: true });
    assert.ok(dict_values(cmd.taskforces).every((t) => t.status === TaskForceStatus.STOPPED));
  });

  tp("test_submit_after_shutdown_raises", (fresh_policy) => {
    const cmd = fresh_policy.central.command;
    cmd.shutdown({ wait: true, cancel_pending: true });
    assert.throws(() => cmd.submit([() => 1]), E.RuntimeError);
  });
});

// ---------------------------------------------------------------------------
// submit() return-shape contract
// ---------------------------------------------------------------------------

describe("TestSubmitShapes", () => {
  tc("test_single_returns_concrete_future", (cmd) => {
    const f = cmd.submit([() => 1]);
    assert.ok(f instanceof ConcurrentPackageFuture);
    assert.ok(f instanceof Future);
    assert.equal(f.wait(WAIT).data, 1);
  });

  tc("test_many_returns_group", (cmd) => {
    const g = cmd.submit([() => 1, () => 2]);
    assert.ok(g instanceof GroupFuture);
    assert.equal(g.__len__(), 2);
    assert.deepEqual(
      g.wait(WAIT).map((e) => e.data),
      [1, 2],
    );
  });

  tc("test_single_wait_true_returns_entry", (cmd) => {
    const r = cmd.submit([() => 5], { wait: true });
    assert.ok(r instanceof Entry);
    assert.equal(r.data, 5);
  });

  tc("test_many_wait_true_returns_entry_list", (cmd) => {
    const r = cmd.submit([() => 1, () => 2, () => 3], { wait: true });
    assert.deepEqual(
      r.map((e) => e.data),
      [1, 2, 3],
    );
  });

  tc("test_empty_submit_returns_empty_group", (cmd) => {
    const g = cmd.submit([]);
    assert.ok(g instanceof GroupFuture);
    assert.equal(g.__len__(), 0);
    assert.deepEqual(g.wait(WAIT), []);
  });

  tc("test_empty_submit_wait_true", (cmd) => {
    assert.deepEqual(cmd.submit([], { wait: true }), []);
  });

  tc("test_generator_tasks_accepted", (cmd) => {
    const g = cmd.submit(
      (function* () {
        for (const i of range(3)) yield () => i;
      })(),
    );
    assert.deepEqual(
      g.wait(WAIT).map((e) => e.data),
      [0, 1, 2],
    );
  });

  tc("test_coroutine_function_task", (cmd) => {
    const co = async () => {
      await asyncio.sleep(0.01);
      return "co";
    };
    assert.equal(cmd.submit([co]).wait(WAIT).data, "co");
  });

  tc("test_partial_of_coroutine_function", (cmd) => {
    const co = async (x) => x * 2;
    assert.equal(cmd.submit([partial(co, 21)]).wait(WAIT).data, 42);
  });

  tc("test_sync_returning_coroutine_is_awaited", (cmd) => {
    const co = async () => "inner";
    assert.equal(cmd.submit([() => co()]).wait(WAIT).data, "inner");
  });

  tc("test_result_entry_passthrough", (cmd) => {
    const e = Entry.constant([1, 2]);
    const f = cmd.submit([() => e]);
    assert.equal(f.wait(WAIT), e);
    assert.equal(f.result_global_id, e.global_id);
  });

  tc("test_none_result", (cmd) => {
    const f = cmd.submit([() => null]);
    f.wait(WAIT);
    assert.ok(f.finished());
    assert.equal(f.result, null);
    assert.equal(f.result_global_id, null);
    assert.throws(() => f.data, E.RuntimeError);
  });

  tc("test_future_group_id_set_on_children", (cmd, fresh_policy) => {
    const g = cmd.submit([() => 1, () => 2]);
    g.wait(WAIT);
    for (const fid of g.future_ids) assert.equal(getitem(fresh_policy.future_bank, fid).future_group_id, g.global_id);
  });

  tc("test_single_future_has_no_group", (cmd) => {
    const f = cmd.submit([() => 1]);
    f.wait(WAIT);
    assert.equal(f.future_group_id, null);
  });

  tc("test_taskforce_id_recorded", (cmd) => {
    const f = cmd.submit([() => 1]);
    assert.equal(f.taskforce_id, cmd.alpha_taskforce);
    f.wait(WAIT);
  });

  ttf("test_target_specific_taskforce", (cmd, tf) => {
    const f = cmd.submit([() => 9], { taskforce_id: tf.global_id });
    assert.equal(f.taskforce_id, tf.global_id);
    assert.equal(f.wait(WAIT).data, 9);
  });

  tc("test_internal_taskforce_target", (cmd) => {
    const f = cmd.submit([() => 3], { taskforce_id: cmd.internal_taskforce });
    assert.equal(f.wait(WAIT).data, 3);
  });

  tc("test_many_submissions_all_complete", (cmd) => {
    const g = cmd.submit(range(200).map((i) => () => i * i));
    assert.deepEqual(
      g.wait(WAIT).map((e) => e.data),
      range(200).map((i) => i * i),
    );
  });

  tc("test_result_of_wait_true_single_is_same_as_future_result", (cmd) => {
    const e = Entry.constant(7);
    assert.equal(cmd.submit([() => e], { wait: true }), e);
  });
});

// ---------------------------------------------------------------------------
// Future lifecycle
// ---------------------------------------------------------------------------

describe("TestFutureLifecycle", () => {
  tc("test_scope_and_gid", (cmd) => {
    const f = cmd.submit([() => 1]);
    assert.ok(f.global_id.startsWith("LAILA:FUTURE:"));
    f.wait(WAIT);
  });

  tc("test_registered_in_future_bank", (cmd, fresh_policy) => {
    const f = cmd.submit([() => 1]);
    assert.equal(getitem(fresh_policy.future_bank, f.global_id), f);
    f.wait(WAIT);
  });

  tc("test_finished_predicates", (cmd) => {
    const f = cmd.submit([() => 1]);
    f.wait(WAIT);
    assert.ok(f.finished() && !f.error() && !f.cancelled() && !f.running());
    assert.ok(!f.not_started());
    assert.equal(f.status, FutureStatus.FINISHED);
  });

  tc("test_error_predicates", (cmd) => {
    const f = cmd.submit([_boom]);
    assert.throws(
      () => f.wait(WAIT),
      (e) => e instanceof E.ValueError && /boom/.test(e.message),
    );
    assert.ok(f.error());
    assert.ok(f.exception instanceof E.ValueError);
    assert.equal(f.status, FutureStatus.ERROR);
  });

  tc("test_async_error_propagates", (cmd) => {
    const f = cmd.submit([_aboom]);
    assert.throws(() => f.wait(WAIT), E.KeyError);
  });

  tc("test_result_reraises_on_error", (cmd) => {
    const f = cmd.submit([_boom]);
    assert.throws(() => f.wait(WAIT), E.ValueError);
    assert.throws(() => f.result, E.ValueError);
    assert.throws(() => f.data, E.ValueError);
  });

  tc("test_wait_is_idempotent", (cmd) => {
    const f = cmd.submit([() => 4]);
    const a = f.wait(WAIT);
    const b = f.wait(WAIT);
    assert.equal(a, b);
  });

  tc("test_result_blocks_until_done", (cmd) => {
    const ev = new TH.Event();
    const f = cmd.submit([
      () => {
        ev.wait(WAIT);
        return "done";
      },
    ]);
    new TH.Timer(0.05, () => ev.set()).start();
    assert.equal(f.result.data, "done");
  });

  tc("test_wait_timeout_raises", (cmd) => {
    const ev = new TH.Event();
    const f = cmd.submit([_blocks_on(ev)]);
    assert.throws(() => f.wait(0.05), E.TimeoutError);
    ev.set();
    f.wait(WAIT);
  });

  tc("test_wait_timeout_leaves_future_non_terminal", (cmd) => {
    // POLL_TIMEOUT is a designed transient state ("the next poll may
    // resolve to any other state"); a timed-out wait must never
    // move the future into a terminal state.
    const ev = new TH.Event();
    const f = cmd.submit([_blocks_on(ev)]);
    assert.throws(() => f.wait(0.05), E.TimeoutError);
    try {
      assert.ok([FutureStatus.RUNNING, FutureStatus.NOT_STARTED, FutureStatus.POLL_TIMEOUT].includes(f.status));
    } finally {
      ev.set();
      f.wait(WAIT);
    }
  });

  tc("test_wait_timeout_sets_poll_timeout_then_recovers", (cmd) => {
    const ev = new TH.Event();
    const f = cmd.submit([_blocks_on(ev)]);
    assert.throws(() => f.wait(0.05), E.TimeoutError);
    assert.equal(f.status, FutureStatus.POLL_TIMEOUT);
    ev.set();
    assert.equal(f.wait(WAIT).data, true);
    assert.equal(f.status, FutureStatus.FINISHED);
  });

  tc("test_group_percentages_sum_to_100_after_timeout", (cmd) => {
    const ev = new TH.Event();
    const g = cmd.submit([_blocks_on(ev), _blocks_on(ev)]);
    assert.throws(() => g.wait(0.05), E.TimeoutError);
    try {
      approx(pct_sum(g.status), 100.0);
    } finally {
      ev.set();
      g.wait(WAIT);
    }
  });

  tc("test_lazy_entry_wrap_is_stable", (cmd) => {
    const f = cmd.submit([() => ({ a: 1 })]);
    f.wait(WAIT);
    const gid1 = f.result_global_id;
    const gid2 = f.result.global_id;
    assert.ok(gid1 === gid2 && gid1.startsWith("LAILA:ENTRY:"));
  });

  tc("test_result_global_id_before_completion_is_none", (cmd) => {
    const ev = new TH.Event();
    const f = cmd.submit([() => ev.wait(WAIT)]);
    assert.equal(f.result_global_id, null);
    ev.set();
    f.wait(WAIT);
  });

  tc("test_exception_none_on_success", (cmd) => {
    const f = cmd.submit([() => 1]);
    f.wait(WAIT);
    assert.equal(f.exception, null);
  });

  tc("test_native_future_unused_by_async_backend", (cmd) => {
    const f = cmd.submit([() => 1]);
    f.wait(WAIT);
    // The asyncio-backed taskforce drives the future by status, not via
    // a concurrent.futures handle.
    assert.equal(f.native_future, null);
  });

  tc("test_release_removes_from_bank", (cmd, fresh_policy) => {
    const f = cmd.submit([() => 1]);
    f.wait(WAIT);
    f.release();
    assert.ok(!dict_has(fresh_policy.future_bank, f.global_id));
  });

  tc("test_release_idempotent", (cmd) => {
    const f = cmd.submit([() => 1]);
    f.wait(WAIT);
    f.release();
    f.release();
  });

  tc("test_release_before_completion_still_completes", (cmd) => {
    const ev = new TH.Event();
    const f = cmd.submit([
      () => {
        ev.wait(WAIT);
        return 11;
      },
    ]);
    f.release();
    ev.set();
    assert.equal(f.wait(WAIT).data, 11);
  });

  tc("test_future_identity_handle", (cmd) => {
    const f = cmd.submit([() => 2]);
    const fi = f.future_identity;
    assert.ok(fi instanceof _LAILA_IDENTIFIABLE_FUTURE);
    assert.equal(fi.global_id, f.global_id);
    assert.equal(fi.wait(WAIT).data, 2);
    assert.equal(fi.status, FutureStatus.FINISHED);
    assert.equal(fi.result, f.result);
    assert.equal(fi.data, 2);
    assert.equal(fi.exception, null);
  });

  tc("test_identity_after_release_keyerror", (cmd) => {
    const f = cmd.submit([() => 2]);
    const fi = f.future_identity;
    f.wait(WAIT);
    f.release();
    assert.throws(() => fi.status, E.KeyError);
  });

  tc("test_identity_str_repr", (cmd) => {
    const f = cmd.submit([() => 2]);
    f.wait(WAIT);
    assert.ok(T.str(f).includes(f.global_id));
    assert.ok(repr(f).includes(f.global_id));
  });

  tc("test_as_dict", (cmd) => {
    const f = cmd.submit([() => 2]);
    f.wait(WAIT);
    const d = f.as_dict();
    assert.equal(getitem(d, "taskforce_id"), f.taskforce_id);
  });

  tc("test_json", (cmd) => {
    const f = cmd.submit([() => 2]);
    f.wait(WAIT);
    assert.ok(f.__json__().includes(f.taskforce_id));
  });

  test("test_many_concurrent_waiters", () =>
    with_fresh_policy((fresh_policy) => {
      const cmd = fresh_policy.central.command;
      const ev = new TH.Event();
      const f = cmd.submit([_blocks_on(ev, 1)]);
      const out = [];
      const waiter = () => {
        out.push(f.wait(WAIT).data);
      };
      const ts = range(8).map(() => new TH.Thread({ target: waiter }));
      for (const th of ts) th.start();
      ev.set();
      for (const th of ts) th.join(WAIT);
      assert.deepEqual(out, range(8).map(() => 1));
    }),
  );

  tp("test_abstract_future_wait_not_implemented", (fresh_policy) => {
    const f = new Future({ taskforce_id: "t", policy_id: fresh_policy.global_id });
    assert.throws(() => f.wait(), E.NotImplementedError);
    f.release();
  });

  tp("test_result_setter_none_clears", (fresh_policy) => {
    const f = new Future({ taskforce_id: "t", policy_id: fresh_policy.global_id });
    f.result = 5;
    assert.notEqual(f.result_global_id, null);
    f.result = null;
    assert.equal(f.result_global_id, null);
    f.release();
  });

  tp("test_status_setter_fires_transition", (fresh_policy) => {
    const f = new Future({ taskforce_id: "t", policy_id: fresh_policy.global_id });
    const seen = [];
    f.add_status_callback(FutureStatus.RUNNING, (fut) => seen.push(fut.status));
    f.status = FutureStatus.RUNNING;
    assert.deepEqual(seen, [FutureStatus.RUNNING]);
    f.release();
  });
});

// ---------------------------------------------------------------------------
// Callbacks
// ---------------------------------------------------------------------------

describe("TestCallbacks", () => {
  tc("test_status_callback_fires_on_finish", (cmd) => {
    const ev = new TH.Event();
    const hits = [];
    const f = cmd.submit([
      () => {
        ev.wait(WAIT);
        return 1;
      },
    ]);
    f.add_status_callback(FutureStatus.FINISHED, (fut) => hits.push(fut.global_id));
    ev.set();
    f.wait(WAIT);
    time.sleep(0.05);
    assert.deepEqual(hits, [f.global_id]);
  });

  tc("test_status_callback_fires_immediately_if_already_there", (cmd) => {
    const f = cmd.submit([() => 1]);
    f.wait(WAIT);
    const hits = [];
    f.add_status_callback(FutureStatus.FINISHED, (_fut) => hits.push(1));
    assert.deepEqual(hits, [1]);
  });

  tc("test_multiple_callbacks_in_order", (cmd) => {
    const f = cmd.submit([() => 1]);
    f.wait(WAIT);
    const order = [];
    f.add_status_callback(FutureStatus.FINISHED, (_fut) => order.push("a"));
    f.add_status_callback(FutureStatus.FINISHED, (_fut) => order.push("b"));
    assert.deepEqual(order, ["a", "b"]);
  });

  tc("test_callback_exception_swallowed", (cmd) => {
    const f = cmd.submit([() => 1]);
    f.wait(WAIT);
    f.add_status_callback(FutureStatus.FINISHED, (_fut) => {
      throw new E.ZeroDivisionError("division by zero"); // 1 / 0
    });
  });

  tc("test_error_callback", (cmd) => {
    const hits = [];
    const ev = new TH.Event();
    const body = () => {
      ev.wait(WAIT);
      throw new E.ValueError("x");
    };
    const f = cmd.submit([body]);
    f.add_status_callback(FutureStatus.ERROR, (fut) => hits.push(fut.exception.constructor.name));
    ev.set();
    assert.throws(() => f.wait(WAIT), E.ValueError);
    time.sleep(0.05);
    assert.deepEqual(hits, ["ValueError"]);
  });

  tc("test_running_callback", (cmd) => {
    const hits = [];
    const ev = new TH.Event();
    const f = cmd.submit([() => ev.wait(WAIT)]);
    f.add_status_callback(FutureStatus.RUNNING, (_fut) => hits.push(1));
    ev.set();
    f.wait(WAIT);
    assert.deepEqual(hits, [1]);
  });

  tc("test_add_callback_stores_in_callbacks_dict", (cmd) => {
    const f = cmd.submit([() => 1]);
    f.wait(WAIT);
    const fn = (_r) => null;
    f.add_callback(FutureStatus.FINISHED, fn);
    assert.equal(getitem(f.callbacks, FutureStatus.FINISHED), fn);
  });

  tc("test_clear_callbacks", (cmd) => {
    const f = cmd.submit([() => 1]);
    f.wait(WAIT);
    f.add_callback(FutureStatus.FINISHED, (_r) => null);
    f.clear_callbacks(FutureStatus.FINISHED);
    assert.ok(!dict_get(f.callbacks, FutureStatus.FINISHED, null));
  });

  tc("test_clear_all_callbacks", (cmd) => {
    const f = cmd.submit([() => 1]);
    f.wait(WAIT);
    f.add_callback(FutureStatus.FINISHED, (_r) => null);
    f.clear_all_callbacks();
    assert.equal(dict_len(f.callbacks), 0);
  });

  tc("test_trigger_callback", (cmd) => {
    const f = cmd.submit([() => 1]);
    f.wait(WAIT);
    const hits = [];
    f.add_callback(FutureStatus.FINISHED, (r) => hits.push(r));
    f.trigger_callback(FutureStatus.FINISHED);
    assert.ok(hits.length);
  });

  tc("test_remove_callback_deletes_key", (cmd) => {
    const f = cmd.submit([() => 1]);
    f.wait(WAIT);
    const fn = (_r) => null;
    f.add_callback(FutureStatus.FINISHED, fn);
    f.remove_callback(FutureStatus.FINISHED, fn);
    assert.ok(!dict_has(f.callbacks, FutureStatus.FINISHED));
  });

  tc("test_remove_callback_stops_firing", (cmd) => {
    const f = cmd.submit([() => 1]);
    f.wait(WAIT);
    const hits = [];
    const fn = (_fut) => hits.push(1);
    f.add_status_callback(FutureStatus.RUNNING, fn);
    f.remove_callback(FutureStatus.RUNNING, fn);
    f.status = FutureStatus.RUNNING;
    assert.deepEqual(hits, []);
  });
});

// ---------------------------------------------------------------------------
// GroupFuture
// ---------------------------------------------------------------------------

describe("TestGroupFuture", () => {
  tc("test_scope", (cmd) => {
    const g = cmd.submit([() => 1, () => 2]);
    assert.ok(g.global_id.startsWith("LAILA:GROUP_FUTURE:"));
    g.wait(WAIT);
  });

  tc("test_iter_and_len", (cmd) => {
    const g = cmd.submit([() => 1, () => 2]);
    assert.deepEqual([...g], g.future_ids);
    assert.equal(g.__len__(), 2);
    g.wait(WAIT);
  });

  tc("test_result_and_data", (cmd) => {
    const g = cmd.submit([() => "a", () => "b"]);
    g.wait(WAIT);
    assert.deepEqual(
      g.result.map((e) => e.data),
      ["a", "b"],
    );
    assert.deepEqual(g.data, ["a", "b"]);
  });

  tc("test_status_breakdown_finished", (cmd) => {
    const g = cmd.submit([() => 1, () => 2]);
    g.wait(WAIT);
    const s = g.status;
    assert.equal(Number(s.total), 2.0);
    assert.equal(Number(s.percentages.finished), 100.0);
    approx(pct_sum(s), 100.0);
  });

  tc("test_status_breakdown_mixed", (cmd) => {
    const g = cmd.submit([() => 1, _boom]);
    assert.throws(() => g.wait(WAIT), E.ValueError);
    const s = g.status.percentages;
    assert.ok(Number(s.finished) === 50.0 && Number(s.error) === 50.0);
  });

  tp("test_status_empty_group", (fresh_policy) => {
    const g = new GroupFuture({ taskforce_id: "t", policy_id: fresh_policy.global_id });
    assert.equal(Number(g.status.percentages.not_started), 100.0);
    assert.equal(Number(g.status.total), 0.0);
    g.release();
  });

  tc("test_wait_raises_first_child_error", (cmd) => {
    const g = cmd.submit([() => 1, _boom, () => 3]);
    assert.throws(
      () => g.wait(WAIT),
      (e) => e instanceof E.ValueError && /boom/.test(e.message),
    );
  });

  tc("test_result_after_error_reraises", (cmd) => {
    const g = cmd.submit([() => 1, _boom]);
    assert.throws(() => g.wait(WAIT), E.ValueError);
    assert.throws(() => g.result, E.ValueError);
  });

  tc("test_wait_timeout", (cmd) => {
    const ev = new TH.Event();
    const g = cmd.submit([_blocks_on(ev), _blocks_on(ev)]);
    assert.throws(() => g.wait(0.05), E.TimeoutError);
    ev.set();
    g.wait(WAIT);
  });

  tc("test_append_extends_ids", (cmd) => {
    const g = cmd.submit([() => 1, () => 2]);
    const f = cmd.submit([() => 3]);
    g.append([f.global_id]);
    assert.equal(g.__len__(), 3);
    assert.equal(g.wait(WAIT)[2].data, 3);
  });

  tc("test_add_merges_ids", (cmd) => {
    const g1 = cmd.submit([() => 1, () => 2]);
    const g2 = cmd.submit([() => 3, () => 4]);
    const g3 = g1.__add__(g2);
    assert.equal(g3.__len__(), 4);
    assert.deepEqual(
      g3.wait(WAIT).map((e) => e.data),
      [1, 2, 3, 4],
    );
  });

  tc("test_add_non_group_not_implemented", (cmd) => {
    const g1 = cmd.submit([() => 1, () => 2]);
    g1.wait(WAIT);
    // ``g1 + 5`` raises TypeError in Python because ``__add__`` answers
    // ``NotImplemented``; JS has no operator dispatch, so the observable
    // contract is that sentinel.
    assert.equal(g1.__add__(5), NotImplemented);
  });

  tc("test_add_merges_in_place_as_documented", (cmd) => {
    // ``__add__`` is documented as "Return self with merged child future
    // IDs" -- the group is a handle, not a value.
    const g1 = cmd.submit([() => 1, () => 2]);
    const g2 = cmd.submit([() => 3, () => 4]);
    const g3 = g1.__add__(g2);
    g3.wait(WAIT);
    assert.equal(g3, g1);
    assert.equal(g1.__len__(), 4);
    assert.deepEqual(
      g3.wait(WAIT).map((e) => e.data),
      [1, 2, 3, 4],
    );
  });

  tc("test_release_removes_group_and_children", (cmd, fresh_policy) => {
    const g = cmd.submit([() => 1, () => 2]);
    g.wait(WAIT);
    const ids = [...g.future_ids];
    g.release();
    assert.ok(!dict_has(fresh_policy.future_bank, g.global_id));
    assert.ok(ids.every((i) => !dict_has(fresh_policy.future_bank, i)));
  });

  tc("test_release_children_false_keeps_children", (cmd, fresh_policy) => {
    const g = cmd.submit([() => 1, () => 2]);
    g.wait(WAIT);
    g.release({ children: false });
    assert.ok(!dict_has(fresh_policy.future_bank, g.global_id));
    assert.ok(g.future_ids.every((i) => dict_has(fresh_policy.future_bank, i)));
  });

  tc("test_release_idempotent", (cmd) => {
    const g = cmd.submit([() => 1, () => 2]);
    g.wait(WAIT);
    g.release();
    g.release();
  });

  tc("test_what_success", (cmd) => {
    const g = cmd.submit([() => 1, () => 2]);
    g.wait(WAIT);
    const w = g.what;
    const body = getitem(w, g.global_id);
    assert.deepEqual(new Set(Object.keys(body.futures)), new Set(g.future_ids));
    assert.deepEqual(body.summary.errors, {});
    assert.deepEqual(body.summary.not_cancelled, g.future_ids);
  });

  tc("test_str_is_json", (cmd) => {
    const g = cmd.submit([() => 1, () => 2]);
    g.wait(WAIT);
    assert.equal(JSON.parse(T.str(g))[g.global_id].taskforce_id, g.taskforce_id);
    assert.equal(repr(g), T.str(g));
  });

  tc("test_what_with_error_child", (cmd) => {
    const g = cmd.submit([() => 1, _boom]);
    assert.throws(() => g.wait(WAIT), E.ValueError);
    const w = getitem(g.what, g.global_id);
    assert.equal(Object.keys(w.summary.errors).length, 1);
  });

  tc("test_repr_with_error_child", (cmd) => {
    const g = cmd.submit([() => 1, _boom]);
    assert.throws(() => g.wait(WAIT), E.ValueError);
    assert.ok(repr(g).includes(g.global_id));
  });

  tc("test_repr_after_release", (cmd) => {
    const g = cmd.submit([() => 1, () => 2]);
    g.wait(WAIT);
    g.release();
    assert.ok(repr(g).includes(g.global_id));
  });

  tc("test_status_after_release", (cmd) => {
    const g = cmd.submit([() => 1, () => 2]);
    g.wait(WAIT);
    g.release();
    const st = g.status;
    assert.equal(Number(st.total), 2.0);
    approx(pct_sum(st), 100.0);
  });

  tc("test_registered_in_bank", (cmd, fresh_policy) => {
    const g = cmd.submit([() => 1, () => 2]);
    assert.equal(getitem(fresh_policy.future_bank, g.global_id), g);
    g.wait(WAIT);
  });

  tc("test_children_resolve_order", (cmd) => {
    const g = cmd.submit([
      () => {
        time.sleep(0.05);
        return "slow";
      },
      () => "fast",
    ]);
    assert.deepEqual(g.data, ["slow", "fast"]);
  });

  tc("test_await_group", async (cmd) => {
    const g = cmd.submit([() => 1, () => 2]);
    const main = async () => await g;
    assert.deepEqual(
      (await main()).map((e) => e.data),
      [1, 2],
    );
  });

  tc("test_await_single", async (cmd) => {
    const f = cmd.submit([() => 1]);
    const main = async () => await f;
    assert.equal((await main()).data, 1);
  });

  tc("test_await_error", async (cmd) => {
    const f = cmd.submit([_boom]);
    const main = async () => await f;
    await assert.rejects(main(), E.ValueError);
  });
});

// ---------------------------------------------------------------------------
// Nested submission, parking and guards
// ---------------------------------------------------------------------------

describe("TestNestedSubmission", () => {
  tc("test_nested_sync_submit", (cmd) => {
    const outer = () => cmd.submit([() => 41]).wait(WAIT).data + 1;
    assert.equal(cmd.submit([outer]).wait(WAIT).data, 42);
  });

  tc("test_nested_async_await", (cmd) => {
    const outer = async () => (await cmd.submit([() => 41])).data + 1;
    assert.equal(cmd.submit([outer]).wait(WAIT).data, 42);
  });

  tc("test_deep_nesting", (cmd) => {
    const level = (n) => {
      if (n === 0) return 0;
      return cmd.submit([() => level(n - 1)]).wait(WAIT).data + 1;
    };
    assert.equal(cmd.submit([() => level(5)]).wait(WAIT).data, 5);
  });

  ttf("test_nesting_beyond_slot_capacity_does_not_deadlock", (cmd, tf) => {
    // tf has 1 worker * 4 slots and 2 sync permits; nest deeper than both.
    const level = (n) => {
      if (n === 0) return 0;
      return cmd.submit([() => level(n - 1)], { taskforce_id: tf.global_id }).wait(WAIT).data + 1;
    };
    assert.equal(cmd.submit([() => level(8)], { taskforce_id: tf.global_id }).wait(WAIT).data, 8);
  });

  ttf("test_fanout_beyond_capacity", (cmd, tf) => {
    const parent = () => {
      const g = cmd.submit(
        range(10).map((i) => () => i),
        { taskforce_id: tf.global_id },
      );
      return g.wait(WAIT).reduce((acc, e) => acc + e.data, 0);
    };
    assert.equal(cmd.submit([parent], { taskforce_id: tf.global_id }).wait(WAIT).data, 45);
  });

  ttf("test_parked_counter_returns_to_zero", (cmd, tf) => {
    const outer = () => cmd.submit([() => 1], { taskforce_id: tf.global_id }).wait(WAIT).data;
    cmd.submit([outer], { taskforce_id: tf.global_id }).wait(WAIT);
    time.sleep(0.1);
    assert.equal(tf.parked, 0);
    assert.equal(tf.inflight, 0);
  });

  tc("test_wait_inside_loop_thread_raises", (cmd) => {
    const outer = async () => {
      const inner = cmd.submit([() => 1]);
      return inner.wait(WAIT);
    };
    const f = cmd.submit([outer]);
    assert.throws(() => f.wait(WAIT), LoopBlockingWaitError);
  });

  tc("test_group_wait_inside_loop_thread_raises", (cmd) => {
    const outer = async () => cmd.submit([() => 1, () => 2]).wait(WAIT);
    assert.throws(() => cmd.submit([outer]).wait(WAIT), LoopBlockingWaitError);
  });

  tc("test_no_command_submit_sync", (cmd) => {
    const guarded = no_command_submit(function guarded() {
      return cmd.submit([() => 1]);
    });
    assert.throws(() => guarded(), NestedCommandSubmitError);
  });

  tc("test_no_command_submit_async", async (cmd) => {
    const guarded = no_command_submit(async function guarded() {
      return cmd.submit([() => 1]);
    });
    await assert.rejects(guarded(), NestedCommandSubmitError);
  });

  tc("test_no_command_submit_resets", (cmd) => {
    const guarded = no_command_submit(function guarded() {
      return 1;
    });
    guarded();
    _check_no_pending_submit_owner();
    const f = cmd.submit([() => 1]);
    f.wait(WAIT);
  });

  test("test_no_command_submit_preserves_name", () => {
    const named = no_command_submit(function named() {});
    assert.equal(named.name, "named");
  });

  tc("test_upward_rank_submission_rejected", (cmd, fresh_policy) => {
    const hi = new PythonAsyncThreadPoolTaskForce({ num_workers: 1, rank: 5, policy_id: fresh_policy.global_id });
    const lo = new PythonAsyncThreadPoolTaskForce({ num_workers: 1, rank: 1, policy_id: fresh_policy.global_id });
    cmd.add_taskforce(hi);
    cmd.add_taskforce(lo);
    try {
      const up = () => cmd.submit([() => 1], { taskforce_id: hi.global_id }).wait(WAIT);
      assert.throws(() => cmd.submit([up], { taskforce_id: lo.global_id }).wait(WAIT), NestedCommandSubmitError);
    } finally {
      hi.shutdown();
      lo.shutdown();
    }
  });

  tc("test_downward_rank_submission_allowed", (cmd, fresh_policy) => {
    const hi = new PythonAsyncThreadPoolTaskForce({ num_workers: 1, rank: 5, policy_id: fresh_policy.global_id });
    const lo = new PythonAsyncThreadPoolTaskForce({ num_workers: 1, rank: 1, policy_id: fresh_policy.global_id });
    cmd.add_taskforce(hi);
    cmd.add_taskforce(lo);
    try {
      const down = () => cmd.submit([() => 7], { taskforce_id: lo.global_id }).wait(WAIT).data;
      assert.equal(cmd.submit([down], { taskforce_id: hi.global_id }).wait(WAIT).data, 7);
    } finally {
      hi.shutdown();
      lo.shutdown();
    }
  });

  ttf("test_equal_rank_submission_allowed", (cmd, tf) => {
    const same = () => cmd.submit([() => 3], { taskforce_id: tf.global_id }).wait(WAIT).data;
    assert.equal(cmd.submit([same], { taskforce_id: tf.global_id }).wait(WAIT).data, 3);
  });

  tc("test_alpha_to_internal_allowed", (cmd) => {
    const down = () => cmd.submit([() => 1], { taskforce_id: cmd.internal_taskforce }).wait(WAIT).data;
    assert.equal(cmd.submit([down]).wait(WAIT).data, 1);
  });

  tc("test_internal_to_alpha_rejected", (cmd) => {
    const up = () => cmd.submit([() => 1], { taskforce_id: cmd.alpha_taskforce }).wait(WAIT).data;
    assert.throws(() => cmd.submit([up], { taskforce_id: cmd.internal_taskforce }).wait(WAIT), NestedCommandSubmitError);
  });

  tc("test_user_thread_unrestricted", (cmd) => {
    // Submitting from a plain thread to any rank is always allowed.
    assert.equal(cmd.submit([() => 1], { taskforce_id: cmd.internal_taskforce }).wait(WAIT).data, 1);
  });
});

describe("TestParkingHelpers", () => {
  test("test_park_sync_outside_slot_plain_call", () =>
    macrotask(() => {
      assert.equal(park_sync((a, b = 2) => a + b, 1, 3), 4);
    }));

  test("test_park_async_outside_slot", async () => {
    const co = async () => 7;
    assert.equal(await park_async(co()), 7);
  });

  test("test_check_resolve_cycle_empty_chain", () => {
    check_resolve_cycle("LAILA:ENTRY:x");
  });

  test("test_check_resolve_cycle_detects", () => {
    const token = _RESOLVE_CHAIN.set(["a", "b"]);
    try {
      assert.throws(() => check_resolve_cycle("b"), CyclicDependencyError);
      check_resolve_cycle("c");
    } finally {
      _RESOLVE_CHAIN.reset(token);
    }
  });

  test("test_resolve_chain_with_no_mutation", () => {
    const token = _RESOLVE_CHAIN.set(["a"]);
    try {
      assert.deepEqual([...resolve_chain_with("b", "c")], ["a", "b", "c"]);
      assert.deepEqual([..._RESOLVE_CHAIN.get()], ["a"]);
    } finally {
      _RESOLVE_CHAIN.reset(token);
    }
  });

  test("test_cyclic_error_is_runtime_error", () => {
    assert.ok(CyclicDependencyError.prototype instanceof E.RuntimeError);
  });

  tc("test_chain_inherited_by_nested_submission", (cmd) => {
    const child = () => _RESOLVE_CHAIN.get();
    const parent = () => {
      const token = _RESOLVE_CHAIN.set(["root"]);
      try {
        return cmd.submit([child]).wait(WAIT).data;
      } finally {
        _RESOLVE_CHAIN.reset(token);
      }
    };
    assert.deepEqual([...cmd.submit([parent]).wait(WAIT).data], ["root"]);
  });

  tc("test_chain_reset_after_task", (cmd) => {
    const child = () => _RESOLVE_CHAIN.get();
    cmd.submit([child]).wait(WAIT);
    assert.deepEqual([..._RESOLVE_CHAIN.get()], []);
  });

  test("test_loop_thread_registry", () => {
    const ident = 987654321;
    _register_async_loop_thread(ident);
    _unregister_async_loop_thread(ident);
    _check_not_loop_thread();
  });

  test("test_check_not_loop_thread_raises_when_registered", () => {
    const ident = TH.get_ident();
    _register_async_loop_thread(ident);
    try {
      assert.throws(() => _check_not_loop_thread(), LoopBlockingWaitError);
    } finally {
      _unregister_async_loop_thread(ident);
    }
  });

  test("test_ensure_coroutine_function_passthrough", () => {
    const co = async () => {};
    assert.equal(ensure_coroutine_function(co), co);
  });

  test("test_ensure_coroutine_function_wraps_sync", async () => {
    const w = ensure_coroutine_function(() => 5);
    assert.ok(asyncio.iscoroutinefunction(w));
    assert.equal(await w(), 5);
  });

  test("test_ensure_coroutine_function_awaits_returned_coroutine", async () => {
    const inner = async () => "x";
    const w = ensure_coroutine_function(() => inner());
    assert.equal(await w(), "x");
  });

  test("test_ensure_coroutine_function_keeps_name", () => {
    assert.equal(ensure_coroutine_function(function named() {}).name, "named");
  });

  tp("test_cycle_detected_through_laila_build", () => {
    // A variable whose body remembers *itself* is on its own resolve
    // chain; this must fail fast with CyclicDependencyError, not hang.
    let e = Entry.variable(null, { nickname: "deep-eval-self-ref" });
    // ``import laila`` inside the body: the root is published on ``globalThis``.
    const slot = "_laila_deep09_laila";
    globalThis[slot] = laila;
    try {
      const src =
        "function f(m) {\n" + //
        `  const laila = globalThis[${JSON.stringify(slot)}];\n` +
        `  return laila.remember(${JSON.stringify(e.global_id)}).wait().data;\n` +
        "}\n";
      e = Entry.variable(null, { nickname: "deep-eval-self-ref", constitution: src, manifest: new Manifest({ data: {} }) });
      const f = laila.build(e);
      const start = time.monotonic();
      assert.throws(() => f.wait(WAIT), CyclicDependencyError);
      assert.ok(time.monotonic() - start < WAIT);
    } finally {
      delete globalThis[slot];
    }
  });
});

// ---------------------------------------------------------------------------
// PythonAsyncThreadPoolTaskForce
// ---------------------------------------------------------------------------

describe("TestTaskForce", () => {
  tp("test_auto_start", (fresh_policy) => {
    const t = new PythonAsyncThreadPoolTaskForce({ num_workers: 1, policy_id: fresh_policy.global_id });
    try {
      assert.equal(t.status, TaskForceStatus.RUNNING);
    } finally {
      t.shutdown();
    }
  });

  ttf("test_scope", (_cmd, tf) => {
    assert.ok(tf.global_id.startsWith("LAILA:TASK_FORCE:"));
  });

  ttf("test_is_identifiable_taskforce", (_cmd, tf) => {
    assert.ok(tf instanceof _LAILA_IDENTIFIABLE_TASK_FORCE);
  });

  tp("test_paused_construction_does_not_start", (fresh_policy) => {
    const t = new PythonAsyncThreadPoolTaskForce({ num_workers: 1, status: TaskForceStatus.PAUSED, policy_id: fresh_policy.global_id });
    try {
      assert.equal(t.status, TaskForceStatus.PAUSED);
      assert.throws(() => t.submit([() => 1]), E.RuntimeError);
    } finally {
      t.shutdown();
    }
  });

  ttf("test_start_idempotent", (_cmd, tf) => {
    const d = tf._dispatcher;
    tf.start();
    assert.equal(tf._dispatcher, d);
  });

  tp("test_start_after_pause_state", (fresh_policy) => {
    const t = new PythonAsyncThreadPoolTaskForce({ num_workers: 1, status: TaskForceStatus.PAUSED, policy_id: fresh_policy.global_id });
    try {
      t.start();
      assert.equal(t.status, TaskForceStatus.RUNNING);
      assert.equal(t.submit([() => 2]).wait(WAIT).data, 2);
    } finally {
      t.shutdown();
    }
  });

  tp("test_context_manager", (fresh_policy) => {
    const t = new PythonAsyncThreadPoolTaskForce({ num_workers: 1, status: TaskForceStatus.PAUSED, policy_id: fresh_policy.global_id });
    with_(t, (inner) => {
      assert.equal(inner, t);
      assert.equal(t.status, TaskForceStatus.RUNNING);
      assert.equal(t.submit([() => 3]).wait(WAIT).data, 3);
    });
    assert.equal(t.status, TaskForceStatus.STOPPED);
  });

  tp("test_context_manager_shuts_down_on_exception", (fresh_policy) => {
    const t = new PythonAsyncThreadPoolTaskForce({ num_workers: 1, policy_id: fresh_policy.global_id });
    assert.throws(
      () =>
        with_(t, () => {
          throw new E.RuntimeError("x");
        }),
      E.RuntimeError,
    );
    assert.equal(t.status, TaskForceStatus.STOPPED);
  });

  tp("test_shutdown_idempotent", (fresh_policy) => {
    const t = new PythonAsyncThreadPoolTaskForce({ num_workers: 1, policy_id: fresh_policy.global_id });
    t.shutdown();
    t.shutdown();
    assert.equal(t.status, TaskForceStatus.STOPPED);
  });

  tp("test_submit_after_shutdown_raises", (fresh_policy) => {
    const t = new PythonAsyncThreadPoolTaskForce({ num_workers: 1, policy_id: fresh_policy.global_id });
    t.shutdown();
    assert.throws(() => t.submit([() => 1]), E.RuntimeError);
  });

  test(
    "test_run_sync_requires_running",
    () =>
      with_fresh_policy((fresh_policy) => {
        const t = new PythonAsyncThreadPoolTaskForce({ num_workers: 1, policy_id: fresh_policy.global_id });
        t.shutdown();
        assert.throws(() => t.run_sync(() => 1), E.RuntimeError);
      }),
  );

  tp("test_live_registry", (fresh_policy) => {
    const t = new PythonAsyncThreadPoolTaskForce({ num_workers: 1, policy_id: fresh_policy.global_id });
    assert.ok([..._live_taskforces_snapshot()].some((x) => x === t));
    t.shutdown();
    assert.ok(![..._live_taskforces_snapshot()].some((x) => x === t));
  });

  tc("test_wait_true_shutdown_lets_inflight_sync_finish", (cmd, fresh_policy) => {
    const t = new PythonAsyncThreadPoolTaskForce({ num_workers: 1, policy_id: fresh_policy.global_id });
    cmd.add_taskforce(t);
    const f = cmd.submit(
      [
        () => {
          time.sleep(0.3);
          return "done";
        },
      ],
      { taskforce_id: t.global_id },
    );
    time.sleep(0.1);
    t.shutdown({ wait: true });
    assert.equal(f.wait(WAIT).data, "done");
  });

  tc("test_wait_true_shutdown_lets_inflight_async_finish", (cmd, fresh_policy) => {
    const t = new PythonAsyncThreadPoolTaskForce({ num_workers: 1, policy_id: fresh_policy.global_id });
    cmd.add_taskforce(t);
    const co = async () => {
      await asyncio.sleep(0.3);
      return "done";
    };
    const f = cmd.submit([co], { taskforce_id: t.global_id });
    time.sleep(0.1);
    t.shutdown({ wait: true });
    assert.equal(f.wait(WAIT).data, "done");
  });

  tc("test_shutdown_without_wait_cancels_inflight_with_runtime_error", (cmd, fresh_policy) => {
    // ``wait=False`` keeps the immediate stop/cancel semantics.
    const t = new PythonAsyncThreadPoolTaskForce({ num_workers: 1, policy_id: fresh_policy.global_id });
    cmd.add_taskforce(t);
    // ``lambda: time.sleep(0.5) or "done"``: a sync ``time.sleep`` in the body
    // would pump underneath the caller's ``time.sleep(0.1)`` (see ``_blocks_on``),
    // completing before ``shutdown`` could ever see it in flight.
    const f = cmd.submit(
      [
        async () => {
          await asyncio.sleep(0.5);
          return "done";
        },
      ],
      { taskforce_id: t.global_id },
    );
    time.sleep(0.1);
    t.shutdown({ wait: false, cancel_pending: true });
    assert.throws(() => f.wait(WAIT), E.RuntimeError);
    assert.equal(f.status, FutureStatus.CANCELLED);
  });

  tc("test_cancel_pending_with_wait_true_cancels_queued", (cmd, fresh_policy) => {
    const t = new PythonAsyncThreadPoolTaskForce({ num_workers: 1, max_async_per_thread: 1, sync_workers: 1, policy_id: fresh_policy.global_id });
    cmd.add_taskforce(t);
    const ev = new TH.Event();
    const blocker = cmd.submit([_blocks_on(ev)], { taskforce_id: t.global_id });
    time.sleep(0.1);
    const queued = range(3).map(() => cmd.submit([() => 1], { taskforce_id: t.global_id }));
    time.sleep(0.1);
    new TH.Timer(0.2, () => ev.set()).start();
    t.shutdown({ wait: true, cancel_pending: true });
    for (const q of queued) {
      assert.throws(() => q.wait(WAIT), E.RuntimeError);
      assert.equal(q.status, FutureStatus.CANCELLED);
      assert.ok(q.exception instanceof E.RuntimeError);
    }
    // ``cancel_pending=True`` is an explicit request to drop work, so the
    // in-flight blocker is cancelled too (graceful drain is ``wait=True``
    // *without* ``cancel_pending``; see test_wait_true_shutdown_lets_*).
    assert.throws(() => blocker.wait(WAIT), E.RuntimeError);
  });

  tc("test_cancel_pending_without_wait_terminates_every_queued_future", (cmd, fresh_policy) => {
    const t = new PythonAsyncThreadPoolTaskForce({ num_workers: 1, max_async_per_thread: 1, sync_workers: 1, policy_id: fresh_policy.global_id });
    cmd.add_taskforce(t);
    const ev = new TH.Event();
    const blocker = cmd.submit([_blocks_on(ev)], { taskforce_id: t.global_id });
    time.sleep(0.1);
    const queued = range(3).map(() => cmd.submit([() => 1], { taskforce_id: t.global_id }));
    time.sleep(0.1);
    t.shutdown({ wait: false, cancel_pending: true });
    ev.set();
    for (const q of queued) {
      try {
        q.wait(2);
      } catch (e) {
        if (e instanceof E.TimeoutError) assert.fail("queued future never terminated after shutdown");
        if (!(e instanceof E.RuntimeError)) throw e;
      }
      assert.ok([FutureStatus.CANCELLED, FutureStatus.FINISHED].includes(q.status));
    }
    try {
      blocker.wait(2);
    } catch {
      /* pass */
    }
  });

  test("test_direct_submit_sync_bodies_use_executor", () =>
    with_fresh_policy((fresh_policy) => {
      const tf = new PythonAsyncThreadPoolTaskForce({ num_workers: 1, max_async_per_thread: 4, sync_workers: 2, policy_id: fresh_policy.global_id });
      fresh_policy.central.command.add_taskforce(tf);
      try {
        const names = tf
          .submit(range(3).map(() => () => TH.current_thread().name))
          .wait(WAIT)
          .map((e) => e.data);
        assert.ok(
          names.every((n) => n.includes("Sync")),
          names.join(","),
        );
      } finally {
        tf.shutdown({ wait: true, cancel_pending: true });
      }
    }),
  );

  test("test_direct_submit_sync_bodies_run_concurrently", () =>
    with_fresh_policy((fresh_policy) => {
      const t = new PythonAsyncThreadPoolTaskForce({ num_workers: 1, sync_workers: 4, policy_id: fresh_policy.global_id });
      try {
        const barrier = new Barrier(4, 3);
        t.submit(range(4).map(() => () => barrier.wait())).wait(WAIT);
      } finally {
        t.shutdown({ wait: false, cancel_pending: true });
      }
    }),
  );

  ttf("test_pause_is_safe", (_cmd, tf) => {
    tf.pause();
    assert.ok([TaskForceStatus.PAUSED, TaskForceStatus.RUNNING].includes(tf.status));
  });

  tp("test_pause_on_not_running_noop", (fresh_policy) => {
    const t = new PythonAsyncThreadPoolTaskForce({ num_workers: 1, status: TaskForceStatus.PAUSED, policy_id: fresh_policy.global_id });
    t.pause();
    assert.equal(t.status, TaskForceStatus.PAUSED);
    t.shutdown();
  });

  ttf("test_queue_len_and_len", (_cmd, tf) => {
    assert.equal(tf.queue_len, 0);
    assert.equal(tf.__len__(), 0);
    assert.ok(!tf.__len__()); // ``not tf``
  });

  ttf("test_queue_drains", (_cmd, tf) => {
    const g = tf.submit(range(20).map(() => () => 1));
    g.wait(WAIT);
    time.sleep(0.05);
    assert.equal(tf.queue_len, 0);
  });

  ttf("test_imap_yields_futures_in_order", (_cmd, tf) => {
    const futs = [...tf.imap(range(5).map((i) => () => i))];
    assert.ok(futs.every((f) => f instanceof ConcurrentPackageFuture));
    assert.deepEqual(
      futs.map((f) => f.wait(WAIT).data),
      [0, 1, 2, 3, 4],
    );
  });

  ttf("test_imap_is_lazy", (_cmd, tf) => {
    const calls = [];
    function* gen() {
      for (const i of range(3)) {
        calls.push(i);
        yield () => i;
      }
    }
    const it = tf.imap(gen());
    assert.deepEqual(calls, []);
    const first = it.next().value;
    assert.deepEqual(calls, [0]);
    assert.equal(first.wait(WAIT).data, 0);
    for (const f of it) f.wait(WAIT);
  });

  ttf("test_submit_single_wait", (_cmd, tf) => {
    assert.equal(tf.submit([() => 8], { wait: true }).data, 8);
  });

  ttf("test_submit_many_wait", (_cmd, tf) => {
    assert.deepEqual(
      tf.submit([() => 1, () => 2], { wait: true }).map((e) => e.data),
      [1, 2],
    );
  });

  tp("test_backend_validation", (fresh_policy) => {
    assert.throws(() => new PythonAsyncThreadPoolTaskForce({ num_workers: 1, backend: "threads", policy_id: fresh_policy.global_id }), E.ValueError);
  });

  tp("test_num_workers_ge_1", (fresh_policy) => {
    assert.throws(() => new PythonAsyncThreadPoolTaskForce({ num_workers: 0, policy_id: fresh_policy.global_id }));
  });

  tp("test_rank_ge_0", (fresh_policy) => {
    assert.throws(() => new PythonAsyncThreadPoolTaskForce({ num_workers: 1, rank: -1, policy_id: fresh_policy.global_id }));
  });

  ttf("test_inflight_zero_when_idle", (_cmd, tf) => {
    assert.equal(tf.inflight, 0);
    assert.equal(tf.parked, 0);
  });

  ttf("test_inflight_counts_running", (_cmd, tf) => {
    const ev = new TH.Event();
    const f = tf.submit([_blocks_on(ev)]);
    time.sleep(0.1);
    assert.equal(tf.inflight, 1);
    ev.set();
    f.wait(WAIT);
  });

  test("test_sync_bodies_run_concurrently", () =>
    with_fresh_policy((fresh_policy) => {
      const cmd = fresh_policy.central.command;
      const t = new PythonAsyncThreadPoolTaskForce({ num_workers: 1, sync_workers: 4, policy_id: fresh_policy.global_id });
      cmd.add_taskforce(t);
      try {
        const barrier = new Barrier(4, WAIT);
        cmd.submit(
          range(4).map(() => () => barrier.wait()),
          { taskforce_id: t.global_id },
        ).wait(WAIT);
      } finally {
        t.shutdown();
      }
    }),
  );

  tc("test_sync_permit_limits_concurrency", (cmd, fresh_policy) => {
    const t = new PythonAsyncThreadPoolTaskForce({ num_workers: 1, sync_workers: 1, policy_id: fresh_policy.global_id });
    cmd.add_taskforce(t);
    try {
      const lock = new TH.Lock();
      const active = [0];
      const peak = [0];
      const body = () => {
        with_(lock, () => {
          active[0] += 1;
          peak[0] = Math.max(peak[0], active[0]);
        });
        time.sleep(0.02);
        with_(lock, () => {
          active[0] -= 1;
        });
      };
      cmd.submit(
        range(6).map(() => body),
        { taskforce_id: t.global_id },
      ).wait(WAIT);
      assert.equal(peak[0], 1);
    } finally {
      t.shutdown();
    }
  });

  test("test_sync_permit_allows_n_concurrent", () =>
    with_fresh_policy((fresh_policy) => {
      const cmd = fresh_policy.central.command;
      const t = new PythonAsyncThreadPoolTaskForce({ num_workers: 1, sync_workers: 3, policy_id: fresh_policy.global_id });
      cmd.add_taskforce(t);
      try {
        const lock = new TH.Lock();
        const active = [0];
        const peak = [0];
        const body = () => {
          with_(lock, () => {
            active[0] += 1;
            peak[0] = Math.max(peak[0], active[0]);
          });
          time.sleep(0.1);
          with_(lock, () => {
            active[0] -= 1;
          });
        };
        cmd.submit(
          range(9).map(() => body),
          { taskforce_id: t.global_id },
        ).wait(WAIT);
        assert.equal(peak[0], 3);
      } finally {
        t.shutdown();
      }
    }),
  );

  tp("test_async_tasks_concurrent_on_single_worker", (fresh_policy) => {
    const t = new PythonAsyncThreadPoolTaskForce({ num_workers: 1, max_async_per_thread: 8, policy_id: fresh_policy.global_id });
    try {
      const co = async () => {
        await asyncio.sleep(0.2);
        return 1;
      };
      const start = time.monotonic();
      t.submit(range(8).map(() => co)).wait(WAIT);
      assert.ok(time.monotonic() - start < 1.2);
    } finally {
      t.shutdown();
    }
  });

  tp("test_slot_cap_limits_async_concurrency", (fresh_policy) => {
    const t = new PythonAsyncThreadPoolTaskForce({ num_workers: 1, max_async_per_thread: 2, policy_id: fresh_policy.global_id });
    try {
      const active = [0];
      const peak = [0];
      const co = async () => {
        active[0] += 1;
        peak[0] = Math.max(peak[0], active[0]);
        await asyncio.sleep(0.05);
        active[0] -= 1;
      };
      t.submit(range(6).map(() => co)).wait(WAIT);
      assert.equal(peak[0], 2);
    } finally {
      t.shutdown();
    }
  });

  test("test_command_submit_sync_bodies_use_executor_threads", () =>
    with_fresh_policy((fresh_policy) => {
      const cmd = fresh_policy.central.command;
      const t = new PythonAsyncThreadPoolTaskForce({ num_workers: 3, policy_id: fresh_policy.global_id });
      cmd.add_taskforce(t);
      try {
        const names = cmd
          .submit(
            range(30).map(() => () => TH.current_thread().name),
            { taskforce_id: t.global_id },
          )
          .wait(WAIT);
        assert.ok(names.every((n) => n.data.includes("Sync")));
      } finally {
        t.shutdown();
      }
    }),
  );

  test("test_direct_submit_sync_runs_on_executor_thread", () =>
    with_fresh_policy((fresh_policy) => {
      const tf = new PythonAsyncThreadPoolTaskForce({ num_workers: 1, max_async_per_thread: 4, sync_workers: 2, policy_id: fresh_policy.global_id });
      fresh_policy.central.command.add_taskforce(tf);
      try {
        const name = tf.submit([() => TH.current_thread().name]).wait(WAIT).data;
        assert.ok(name.includes("Sync"), name);
      } finally {
        tf.shutdown({ wait: true, cancel_pending: true });
      }
    }),
  );

  tp("test_async_tasks_spread_across_loops", (fresh_policy) => {
    const t = new PythonAsyncThreadPoolTaskForce({ num_workers: 3, policy_id: fresh_policy.global_id });
    try {
      const co = async () => {
        await asyncio.sleep(0.05);
        return TH.current_thread().name;
      };
      const names = new Set(
        t
          .submit(range(30).map(() => co))
          .wait(WAIT)
          .map((e) => e.data),
      );
      assert.equal(names.size, 3);
    } finally {
      t.shutdown();
    }
  });

  ttf("test_async_task_runs_on_loop_thread", (_cmd, tf) => {
    const co = async () => TH.current_thread().name;
    assert.ok(tf.submit([co]).wait(WAIT).data.includes("Loop"));
  });

  ttf("test_task_exceptions_isolated", (_cmd, tf) => {
    const g = tf.submit([_boom, () => 1]);
    assert.throws(() => g.wait(WAIT), E.ValueError);
    // The group wait raised on the first child; let the sibling (which
    // runs concurrently on the executor) settle before inspecting.
    const bank = laila.active_policy.future_bank;
    for (const fid of g.future_ids) {
      try {
        getitem(bank, fid).wait(WAIT);
      } catch (e) {
        if (!(e instanceof E.ValueError)) throw e;
      }
    }
    assert.equal(Number(g.status.percentages.finished), 50.0);
    assert.equal(Number(g.status.percentages.error), 50.0);
  });

  ttf("test_policy_id_on_future", (_cmd, tf, fresh_policy) => {
    const f = tf.submit([() => 1]);
    assert.equal(f.policy_id, fresh_policy.global_id);
    f.wait(WAIT);
  });

  test("test_submit_from_many_threads", () =>
    with_fresh_policy((fresh_policy) => {
      const tf = new PythonAsyncThreadPoolTaskForce({ num_workers: 1, max_async_per_thread: 4, sync_workers: 2, policy_id: fresh_policy.global_id });
      fresh_policy.central.command.add_taskforce(tf);
      try {
        const results = [];
        const lock = new TH.Lock();
        const worker = (i) => {
          const v = tf.submit([() => i]).wait(WAIT).data;
          with_(lock, () => {
            results.push(v);
          });
        };
        const ts = range(16).map((i) => new TH.Thread({ target: worker, args: [i] }));
        for (const th of ts) th.start();
        for (const th of ts) th.join(WAIT);
        assert.deepEqual(
          [...results].sort((a, b) => a - b),
          range(16),
        );
      } finally {
        tf.shutdown({ wait: true, cancel_pending: true });
      }
    }),
  );

  tp("test_base_hooks_not_implemented", (fresh_policy) => {
    class Bare extends _LAILA_IDENTIFIABLE_TASK_FORCE {
      _on_start() {}

      _on_shutdown(_opts) {}
    }

    const b = new Bare({ policy_id: fresh_policy.global_id });
    try {
      assert.throws(() => b.submit([() => 1]), E.NotImplementedError);
      assert.throws(() => [...b.imap([() => 1])], E.NotImplementedError);
      assert.throws(() => b.pause(), E.NotImplementedError);
    } finally {
      b.shutdown();
    }
  });

  tp("test_base_start_failure_keeps_state", (fresh_policy) => {
    class Broken extends _LAILA_IDENTIFIABLE_TASK_FORCE {
      _on_start() {
        throw new E.RuntimeError("no");
      }
    }

    assert.throws(() => new Broken({ policy_id: fresh_policy.global_id }), E.RuntimeError);
  });
});

// ---------------------------------------------------------------------------
// laila.guarantee / guarantee_async
// ---------------------------------------------------------------------------

describe("TestGuarantee", () => {
  tc("test_waits_for_futures", (cmd) => {
    const ev = new TH.Event();
    new TH.Timer(0.05, () => ev.set()).start();
    let f;
    with_(laila.guarantee, () => {
      f = cmd.submit([() => ev.wait(WAIT)]);
    });
    assert.ok(f.finished());
  });

  tc("test_reraises_future_error", (cmd) => {
    assert.throws(
      () =>
        with_(laila.guarantee, () => {
          cmd.submit([_boom]);
        }),
      (e) => e instanceof E.ValueError && /boom/.test(e.message),
    );
  });

  tc("test_body_exception_takes_precedence", (cmd) => {
    assert.throws(
      () =>
        with_(laila.guarantee, () => {
          cmd.submit([_boom]);
          throw new E.KeyError("body");
        }),
      E.KeyError,
    );
  });

  tc("test_nested_scopes", (cmd) => {
    let outer, inner;
    with_(laila.guarantee, () => {
      outer = cmd.submit([() => 1]);
      with_(laila.guarantee, () => {
        inner = cmd.submit([() => 2]);
      });
      assert.ok(inner.finished());
    });
    assert.ok(outer.finished());
  });

  tc("test_group_futures_tracked", (cmd) => {
    let g;
    with_(laila.guarantee, () => {
      g = cmd.submit([() => 1, () => 2]);
    });
    assert.equal(Number(g.status.percentages.finished), 100.0);
  });

  tc("test_first_error_reported", (cmd) => {
    assert.throws(
      () =>
        with_(laila.guarantee, () => {
          cmd.submit([_boom]);
          cmd.submit([_aboom]);
        }),
      E.ValueError,
    );
  });

  tc("test_stack_empty_after_exit", (cmd) => {
    with_(laila.guarantee, () => {});
    assert.deepEqual(cmd._guarantee_stack(), []);
  });

  tc("test_exit_without_enter_returns_empty", (cmd) => {
    assert.deepEqual(cmd._guarantee_exit(), []);
  });

  tc("test_thread_isolation", (cmd) => {
    const seen = {};
    const other = () => {
      seen.stack = [...cmd._guarantee_stack()];
    };
    with_(laila.guarantee, () => {
      cmd.submit([() => 1]);
      const th = new TH.Thread({ target: other });
      th.start();
      th.join(WAIT);
    });
    assert.deepEqual(seen.stack, []);
  });

  tp("test_memorize_inside_guarantee", () => {
    const e = Entry.constant(123);
    with_(laila.guarantee, () => {
      laila.memorize(e);
    });
    assert.equal(laila.remember(e.global_id).wait(WAIT).data, 123);
  });

  test("test_guarantee_returns_self", () =>
    macrotask(() => {
      with_(laila.guarantee, (g) => {
        assert.equal(g, laila.guarantee);
      });
    }));

  tc("test_async_guarantee_waits", async (cmd) => {
    const main = async () => {
      let f;
      await with_async(laila.guarantee_async, async () => {
        f = cmd.submit([() => 1]);
      });
      return f.finished();
    };
    assert.ok(await run(main) /* the Task starts the coroutine, like asyncio.run(main()) */);
  });

  tc("test_async_guarantee_reraises", async (cmd) => {
    const main = async () => {
      await with_async(laila.guarantee_async, async () => {
        cmd.submit([_boom]);
      });
    };
    await assert.rejects(run(main) /* the Task starts the coroutine, like asyncio.run(main()) */, E.ValueError);
  });

  tc("test_async_guarantee_body_error_precedence", async (cmd) => {
    const main = async () => {
      await with_async(laila.guarantee_async, async () => {
        cmd.submit([_boom]);
        throw new E.KeyError("body");
      });
    };
    await assert.rejects(run(main) /* the Task starts the coroutine, like asyncio.run(main()) */, (e) => e instanceof E.KeyError || e instanceof E.ValueError);
  });

  tc("test_async_guarantee_nested", async (cmd) => {
    const main = async () => {
      let a, b;
      await with_async(laila.guarantee_async, async () => {
        a = cmd.submit([() => 1]);
        await with_async(laila.guarantee_async, async () => {
          b = cmd.submit([() => 2]);
        });
        assert.ok(b.finished());
      });
      return a.finished();
    };
    assert.ok(await run(main) /* the Task starts the coroutine, like asyncio.run(main()) */);
  });

  test(
    "test_async_guarantee_cancels_hung_body_on_error",
    () =>
      with_fresh_policy(async (fresh_policy) => {
        const cmd = fresh_policy.central.command;
        const main = async () => {
          await with_async(laila.guarantee_async, async () => {
            cmd.submit([_boom]);
            await asyncio.sleep(5);
          });
        };
        const start = time.monotonic();
        await assert.rejects(run(main) /* the Task starts the coroutine, like asyncio.run(main()) */, E.ValueError);
        assert.ok(time.monotonic() - start < 4);
      }),
  );

  test("test_async_guarantee_exit_without_enter", async () => {
    const main = async () => await laila.guarantee_async.__aexit__(null, null, null);
    assert.equal(await run(main) /* the Task starts the coroutine, like asyncio.run(main()) */, false);
  });
});

// ---------------------------------------------------------------------------
// Runtime helpers
// ---------------------------------------------------------------------------

describe("TestRuntimeHelpers", () => {
  tc("test_status_by_object", (cmd) => {
    const f = cmd.submit([() => 1]);
    f.wait(WAIT);
    assert.equal(laila.runtime.status(f), FutureStatus.FINISHED);
    assert.equal(laila.status(f), FutureStatus.FINISHED);
  });

  tc("test_status_by_gid", (cmd) => {
    const f = cmd.submit([() => 1]);
    f.wait(WAIT);
    assert.equal(laila.runtime.status(f.global_id), FutureStatus.FINISHED);
  });

  tc("test_status_by_identity", (cmd) => {
    const f = cmd.submit([() => 1]);
    f.wait(WAIT);
    assert.equal(laila.runtime.status(f.future_identity), FutureStatus.FINISHED);
  });

  tc("test_result_and_wait", (cmd) => {
    const f = cmd.submit([() => 2]);
    assert.equal(laila.runtime.wait(f.global_id, WAIT).data, 2);
    assert.equal(laila.runtime.result(f).data, 2);
  });

  tc("test_exception", (cmd) => {
    const f = cmd.submit([_boom]);
    assert.throws(() => f.wait(WAIT), E.ValueError);
    assert.ok(laila.runtime.exception(f.global_id) instanceof E.ValueError);
  });

  tc("test_group_status", (cmd) => {
    const g = cmd.submit([() => 1, () => 2]);
    g.wait(WAIT);
    assert.equal(Number(laila.runtime.status(g.global_id).total), 2.0);
  });

  tc("test_group_result", (cmd) => {
    const g = cmd.submit([() => 1, () => 2]);
    g.wait(WAIT);
    assert.deepEqual(
      laila.runtime.result(g).map((e) => e.data),
      [1, 2],
    );
  });

  tc("test_group_exception", (cmd) => {
    const g = cmd.submit([() => 1, () => 2]);
    g.wait(WAIT);
    assert.equal(laila.runtime.exception(g), null);
  });

  test("test_unknown_gid_keyerror", () =>
    macrotask(() => {
      assert.throws(() => laila.runtime.status("LAILA:FUTURE:00000000-0000-0000-0000-000000000000"), E.KeyError);
    }));

  tc("test_released_gid_keyerror", (cmd) => {
    const f = cmd.submit([() => 1]);
    f.wait(WAIT);
    f.release();
    assert.throws(() => laila.runtime.status(f.global_id), E.KeyError);
  });

  test("test_unsupported_type", () =>
    macrotask(() => {
      assert.throws(() => laila.runtime.status(123), TypeError);
    }));

  tc("test_wait_timeout", (cmd) => {
    const ev = new TH.Event();
    const f = cmd.submit([_blocks_on(ev)]);
    assert.throws(() => laila.runtime.wait(f, 0.05), E.TimeoutError);
    ev.set();
    f.wait(WAIT);
  });

  tc("test_cross_policy_lookup", (cmd, fresh_policy) => {
    const f = cmd.submit([() => 1]);
    f.wait(WAIT);
    const other = new _LAILA_IDENTIFIABLE_POLICY();
    try {
      laila.activate_policy(other);
      assert.equal(laila.runtime.status(f.global_id), FutureStatus.FINISHED);
    } finally {
      laila.activate_policy(fresh_policy);
      other.central.command.shutdown({ wait: true, cancel_pending: true });
    }
  });
});

// Tear down the lazily-activated default policy so the process exits.
test("zz teardown", () =>
  macrotask(() => {
    laila.terminate();
    fs.rmSync(TMP_ROOT, { recursive: true, force: true });
  }));
