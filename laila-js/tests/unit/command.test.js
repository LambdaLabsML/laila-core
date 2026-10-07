/**
 * Central command sub-package: ports of
 *   tests/functional/policy/command/concurrent_package/future/test_cpf_future.py
 *   tests/functional/policy/command/schema/future/unit_tests/future_base_class_tests.py
 *   tests/functional/policy/command/schema/future/unit_tests/group_future_base_class_tests.py
 *   tests/functional/policy/command/schema/future/unit_tests/test_complex_future.py
 *   tests/functional/policy/command/schema/future/unit_tests/test_future_release_and_lazy_result.py
 *   tests/functional/policy/command/concurrent_package/unit_tests/test_python_async_thread_pool_task_force.py
 *   tests/functional/policy/command/concurrent_package/unit_tests/test_parking.py
 *   tests/functional/policy/command/concurrent_package/unit_tests/test_python_process_pool_task_force.py
 *
 * Tests that need central memory (``laila.memorize`` / ``laila.remember`` /
 * ``Manifest``) live in the functional tree once p6 lands:
 *   release 007/008, submit-return-type 003/005 (Manifest half), parking 007/022/023.
 */
import { test, describe } from "node:test";
import assert from "node:assert/strict";
import crypto from "node:crypto";

const S = new URL("../../src/", import.meta.url).href;
const lazy_mod = await import(S + "_compat/lazy.js");
const defaults = await import(S + "macros/defaults.js");
const { LAILA_UNIVERSAL_NAMESPACE } = defaults;
const E = await import(S + "_compat/errors.js");
const T = await import(S + "_compat/pytypes.js");
const asyncio = await import(S + "_compat/asyncio.js");
const time = await import(S + "_compat/time.js");
const TH = await import(S + "_compat/threading.js");
const { partial } = await import(S + "_compat/functools.js");
const { with_ } = await import(S + "_compat/contextlib.js");
const { ConcurrentFuture } = await import(S + "_compat/executor.js");
const { ValidationError } = await import(S + "_compat/pydantic.js");
const ENT = await import(S + "entry/index.js");
lazy_mod.register("laila.entry", ENT);
const { Entry } = ENT;

// ``laila`` root is p8: the command layer needs the active policy accessors.
const _local_policies = {};
let _active = null;
const stub_comm = () => ({ _local_policy: null, stop() {} });
const LAILA = {
  get_active_namespace: () => LAILA_UNIVERSAL_NAMESPACE,
  _local_policies,
  get_active_policy() {
    if (_active === null) {
      const p = new defaults.DefaultPolicy({ central: { memory: {}, communication: stub_comm() } });
      _active = p.global_id;
      _local_policies[p.global_id] = p;
    }
    return _local_policies[_active];
  },
  _get_active_local_policy() {
    return LAILA.get_active_policy();
  },
  get active_policy() {
    return LAILA.get_active_policy();
  },
  get command() {
    return LAILA.get_active_policy().central.command;
  },
};
lazy_mod.register("laila", LAILA);

const CMD = await import(S + "policy/central/command/index.js");
const { Future, FutureStatus, GroupFuture, ComplexFuture, _LAILA_IDENTIFIABLE_FUTURE } = CMD;
const { ConcurrentPackageFuture, ProcessPackageFuture, PythonAsyncThreadPoolTaskForce, PythonProcessPoolTaskForce } = CMD;
const { NestedCommandSubmitError, CyclicDependencyError, _CURRENT_SLOT, _RESOLVE_CHAIN } = CMD;
const PP = await import("./fixtures/process_pool_tasks.js");

const { NotImplemented, getitem, dict_has, dict_len, PyTuple } = T;
const _T = 30.0;

/**
 * Run a body on a fresh macrotask: ``node:test`` invokes test bodies from a
 * microtask, where a blocking wait (``Future.wait``) is impossible by
 * construction (nothing can settle until the job returns).
 */
function macrotask(fn) {
  return new Promise((resolve, reject) =>
    setImmediate(() => {
      try {
        resolve(fn());
      } catch (e) {
        reject(e);
      }
    }),
  );
}

const _unwrap = (v) => {
  while (v !== null && v !== undefined && typeof v === "object" && "data" in v) v = v.data;
  return v;
};

const _bank = () => LAILA.get_active_policy().future_bank;

// ---------------------------------------------------------------------------
// test_cpf_future.py
// ---------------------------------------------------------------------------
describe("TestConcurrentPackageFuture", () => {
  const _make_cpf = () => new ConcurrentPackageFuture({ taskforce_id: "LAILA:TASK_FORCE:test-taskforce", policy_id: "LAILA:POLICY:test-policy" });

  test("001 construct without native future allowed", () => {
    const fut = _make_cpf();
    assert.equal(fut.native_future, null);
  });

  test("002 native future setter once only", () => {
    const fut = _make_cpf();
    fut.native_future = new ConcurrentFuture();
    assert.throws(() => {
      fut.native_future = new ConcurrentFuture();
    }, E.RuntimeError);
  });

  test("003 wait without native times out", () =>
    macrotask(() => {
      const fut = _make_cpf();
      assert.throws(() => fut.wait(0.02), E.TimeoutError);
      assert.equal(fut.status, FutureStatus.POLL_TIMEOUT);
    }));

  test("004 wait without native finished returns value", () =>
    macrotask(() => {
      const fut = _make_cpf();
      fut.status = FutureStatus.FINISHED;
      fut.result = { ok: 1 };
      assert.deepEqual(fut.wait(0.02).data, { ok: 1 });
    }));

  test("005 wait without native error raises exception", () =>
    macrotask(() => {
      const fut = _make_cpf();
      fut.exception = new E.ValueError("boom");
      fut.status = FutureStatus.ERROR;
      assert.throws(() => fut.wait(0.02), E.ValueError);
    }));

  test("006 wait with native success returns value", () =>
    macrotask(() => {
      const fut = _make_cpf();
      const n = new ConcurrentFuture();
      fut.native_future = n;
      n.set_result(7);
      assert.equal(fut.wait(0.5), 7);
    }));

  test("007 wait with native pending timeout raises", () =>
    macrotask(() => {
      const fut = _make_cpf();
      fut.native_future = new ConcurrentFuture();
      assert.throws(() => fut.wait(0.01), E.TimeoutError);
    }));

  test("008 done callback sets finished and result", () =>
    macrotask(() => {
      const fut = _make_cpf();
      const n = new ConcurrentFuture();
      fut.native_future = n;
      n.set_result("ok");
      time.sleep(0.01);
      assert.equal(fut.status, FutureStatus.FINISHED);
      assert.equal(fut.data, "ok");
    }));

  test("009 done callback sets cancelled", () =>
    macrotask(() => {
      const fut = _make_cpf();
      const n = new ConcurrentFuture();
      fut.native_future = n;
      n.cancel();
      time.sleep(0.01);
      assert.equal(fut.status, FutureStatus.CANCELLED);
    }));

  test("010 done callback sets error and exception", () =>
    macrotask(() => {
      const fut = _make_cpf();
      const n = new ConcurrentFuture();
      fut.native_future = n;
      const err = new E.RuntimeError("fail");
      n.set_exception(err);
      time.sleep(0.01);
      assert.equal(fut.status, FutureStatus.ERROR);
      assert.equal(fut.exception, err);
    }));

  test("011 result blocks until native completes", () =>
    macrotask(() => {
      const fut = _make_cpf();
      const n = new ConcurrentFuture();
      fut.native_future = n;
      const t = new TH.Thread({
        target: () => {
          time.sleep(0.02);
          n.set_result("later");
        },
      });
      t.start();
      assert.equal(fut.data, "later");
      t.join(1.0);
    }));

  test("012 wait not started then native assigned", () =>
    macrotask(() => {
      const fut = _make_cpf();
      const t = new TH.Thread({
        target: () => {
          time.sleep(0.02);
          const n = new ConcurrentFuture();
          fut.native_future = n;
          time.sleep(0.02);
          n.set_result("done");
        },
      });
      t.start();
      assert.equal(fut.wait(0.5), "done");
      t.join(1.0);
    }));

  test("013 predicates initial state", () => {
    const fut = _make_cpf();
    assert.ok(fut.not_started());
    assert.ok(!fut.running());
    assert.ok(!fut.finished());
    assert.ok(!fut.error());
    assert.ok(!fut.cancelled());
  });

  test("014 predicate finished after success", () =>
    macrotask(() => {
      const fut = _make_cpf();
      const n = new ConcurrentFuture();
      fut.native_future = n;
      n.set_result(1);
      time.sleep(0.01);
      assert.ok(fut.finished());
    }));

  test("015 predicate cancelled after cancel", () =>
    macrotask(() => {
      const fut = _make_cpf();
      const n = new ConcurrentFuture();
      fut.native_future = n;
      n.cancel();
      time.sleep(0.01);
      assert.ok(fut.cancelled());
    }));

  test("016 predicate error after exception", () =>
    macrotask(() => {
      const fut = _make_cpf();
      const n = new ConcurrentFuture();
      fut.native_future = n;
      n.set_exception(new E.ValueError("bad"));
      time.sleep(0.01);
      assert.ok(fut.error());
    }));
});

// ---------------------------------------------------------------------------
// future_base_class_tests.py
// ---------------------------------------------------------------------------
describe("TestFutureBaseClass", () => {
  const _make_future = () => new Future({ taskforce_id: "LAILA:TASK_FORCE:test-taskforce", policy_id: "LAILA:POLICY:test-policy" });

  test("001 requires identity fields", () => {
    assert.throws(() => new Future(), ValidationError);
  });

  test("002 default status and predicates", () => {
    const fut = _make_future();
    assert.equal(fut.status, FutureStatus.NOT_STARTED);
    assert.ok(fut.not_started());
    assert.ok(!fut.running());
    assert.ok(!fut.finished());
    assert.ok(!fut.error());
    assert.ok(!fut.cancelled());
  });

  test("003 status setter updates predicates", () => {
    const fut = _make_future();
    fut.status = FutureStatus.RUNNING;
    assert.ok(fut.running());
    fut.status = FutureStatus.FINISHED;
    assert.ok(fut.finished());
    fut.status = FutureStatus.ERROR;
    assert.ok(fut.error());
    fut.status = FutureStatus.CANCELLED;
    assert.ok(fut.cancelled());
  });

  test("004 result requires concrete wait when not finished", () => {
    const fut = _make_future();
    assert.throws(() => fut.result, E.NotImplementedError);
  });

  test("005 result roundtrip when marked finished", () => {
    const fut = _make_future();
    fut.result = { ok: 1 };
    fut.status = FutureStatus.FINISHED;
    assert.ok(fut.result instanceof Entry);
    assert.deepEqual(fut.data, { ok: 1 });
  });

  test("006 result raises exception for error status", () => {
    const fut = _make_future();
    fut.exception = new E.RuntimeError("boom");
    fut.status = FutureStatus.ERROR;
    assert.throws(() => fut.result, E.RuntimeError);
  });

  test("007 result raises exception for cancelled status", () => {
    const fut = _make_future();
    fut.exception = new E.RuntimeError("cancelled");
    fut.status = FutureStatus.CANCELLED;
    assert.throws(() => fut.result, E.RuntimeError);
  });

  test("008 exception property roundtrip", () => {
    const fut = _make_future();
    const err = new E.ValueError("x");
    fut.exception = err;
    assert.equal(fut.exception, err);
  });

  test("009 callback add and remove", () => {
    const fut = _make_future();
    const _cb = (_) => null;
    fut.add_callback(FutureStatus.FINISHED, _cb);
    assert.equal(getitem(fut.callbacks, FutureStatus.FINISHED), _cb);
    fut.remove_callback(FutureStatus.FINISHED, _cb);
    // ``remove_callback`` pops the slot (the Python test's
    // ``callbacks[FINISHED] is None`` raises ``KeyError`` upstream as well).
    assert.ok(!dict_has(fut.callbacks, FutureStatus.FINISHED));
    assert.throws(() => getitem(fut.callbacks, FutureStatus.FINISHED), E.KeyError);
  });

  test("010 identity and global_id are available", () => {
    const fut = _make_future();
    assert.ok(fut.global_id.startsWith("LAILA:"));
    assert.ok(fut.global_id.includes(":FUTURE:"));
    const ident = fut.identity();
    assert.ok(dict_has(ident, "uuid"));
  });

  test("011 data returns entry payload for raw result", () => {
    const fut = _make_future();
    fut.result = { key: "value" };
    fut.status = FutureStatus.FINISHED;
    assert.deepEqual(fut.data, { key: "value" });
  });

  test("012 data returns entry payload for entry result", () => {
    const fut = _make_future();
    fut.result = Entry.constant([1, 2, 3]);
    fut.status = FutureStatus.FINISHED;
    assert.deepEqual(fut.data, [1, 2, 3]);
  });

  test("013 data raises when result is None", () => {
    const fut = _make_future();
    fut._return_value = null;
    fut.status = FutureStatus.FINISHED;
    assert.throws(() => fut.data, E.RuntimeError);
  });
});

// ---------------------------------------------------------------------------
// group_future_base_class_tests.py
// ---------------------------------------------------------------------------
describe("TestGroupFutureBaseClass", () => {
  const _mk_child = () => new Future({ taskforce_id: "LAILA:TASK_FORCE:child", policy_id: "LAILA:POLICY:child" });
  const _make_group = (children = null) =>
    new GroupFuture({
      taskforce_id: "LAILA:TASK_FORCE:test-taskforce",
      policy_id: "LAILA:POLICY:test-policy",
      future_ids: children && children.length ? children.map((f) => f.global_id) : [],
    });
  const close = (a, b) => assert.ok(Math.abs(Number(a) - Number(b)) < 1e-7, `${a} !~ ${b}`);

  test("001 requires identity fields", () => {
    assert.throws(() => new GroupFuture(), ValidationError);
  });

  test("002 empty group status shape", () => {
    const s = _make_group().status;
    assert.equal(Number(s.total), 0.0);
    assert.equal(Number(s.percentages.not_started), 100.0);
  });

  test("003 status percentages from children", () => {
    const [f1, f2, f3] = [_mk_child(), _mk_child(), _mk_child()];
    f1.status = FutureStatus.FINISHED;
    f2.status = FutureStatus.RUNNING;
    f3.status = FutureStatus.ERROR;
    const s = _make_group([f1, f2, f3]).status;
    assert.equal(Number(s.total), 3.0);
    close(s.percentages.finished, 100.0 / 3.0);
    close(s.percentages.running, 100.0 / 3.0);
    close(s.percentages.error, 100.0 / 3.0);
  });

  test("004 append merges children", () => {
    const g = _make_group();
    g.append([_mk_child().global_id]);
    g.append([_mk_child().global_id]);
    assert.equal(g.__len__(), 2);
  });

  test("005 add merges in place", () => {
    const g1 = _make_group([_mk_child()]);
    const g2 = _make_group([_mk_child()]);
    const out = g1.__add__(g2);
    assert.equal(out, g1);
    assert.equal(g1.__len__(), 2);
  });

  test("006 add non group returns NotImplemented", () => {
    assert.equal(_make_group().__add__(123), NotImplemented);
  });

  test("007 wait requires children with wait", () =>
    macrotask(() => {
      const g = _make_group([_mk_child()]);
      assert.throws(() => g.wait(0.01), E.RuntimeError);
    }));

  test("008 what includes summary", () => {
    const f = _mk_child();
    f.status = FutureStatus.FINISHED;
    const g = _make_group([f]);
    const payload = getitem(g.what, g.global_id);
    assert.ok(dict_has(payload, "status"));
    assert.ok(dict_has(payload, "summary"));
    assert.ok(dict_has(payload, "futures"));
  });

  test("009 repr and str are json", () => {
    const g = _make_group();
    assert.equal(typeof JSON.parse(g.__str__()), "object");
    assert.equal(typeof JSON.parse(g.__repr__()), "object");
  });

  test("010 len and iter", () => {
    const g = _make_group([_mk_child(), _mk_child()]);
    assert.equal([...g].length, 2);
    assert.equal(g.__len__(), 2);
  });

  test("011 data returns all child payloads", () => {
    const [f1, f2] = [_mk_child(), _mk_child()];
    f1.result = "hello";
    f2.result = "world";
    f1.status = FutureStatus.FINISHED;
    f2.status = FutureStatus.FINISHED;
    assert.deepEqual(_make_group([f1, f2]).data, ["hello", "world"]);
  });

  test("012 data returns entry payloads for entry results", () => {
    const [f1, f2] = [_mk_child(), _mk_child()];
    f1.result = Entry.constant({ a: 1 });
    f2.result = Entry.constant({ b: 2 });
    f1.status = FutureStatus.FINISHED;
    f2.status = FutureStatus.FINISHED;
    assert.deepEqual(_make_group([f1, f2]).data, [{ a: 1 }, { b: 2 }]);
  });

  test("013 data raises when any child result is None", () => {
    const [f1, f2] = [_mk_child(), _mk_child()];
    f1.result = "ok";
    f1.status = FutureStatus.FINISHED;
    f2._return_value = null;
    f2.status = FutureStatus.FINISHED;
    const g = _make_group([f1, f2]);
    assert.throws(() => g.data, E.RuntimeError);
  });
});

// ---------------------------------------------------------------------------
// test_complex_future.py
// ---------------------------------------------------------------------------
describe("TestComplexFuture", () => {
  const _ids = () => {
    const cmd = LAILA.active_policy.central.command;
    const alpha = cmd.alpha_taskforce;
    return [cmd, alpha, alpha, LAILA.active_policy.global_id];
  };

  test("001 two stage success", () =>
    macrotask(() => {
      const [cmd, compute_id, io_id, policy_gid] = _ids();
      const s1 = (_) => cmd.submit([() => 7], { taskforce_id: compute_id });
      const s2 = (prev) => {
        const v = prev.data;
        return cmd.submit([() => v * 6], { taskforce_id: compute_id });
      };
      const cf = new ComplexFuture({ taskforce_id: io_id, policy_id: policy_gid, stage_fns: [s1, s2] });
      const out = cf.wait(10);
      assert.equal(out.data, 42);
      assert.equal(cf.status, FutureStatus.FINISHED);
      assert.equal(cf.stage_future_ids.length, 2);
    }));

  test("002 single stage success", () =>
    macrotask(() => {
      const [cmd, compute_id, io_id, policy_gid] = _ids();
      const s1 = (_) => cmd.submit([() => "hi"], { taskforce_id: compute_id });
      const cf = new ComplexFuture({ taskforce_id: io_id, policy_id: policy_gid, stage_fns: [s1] });
      const out = cf.wait(5);
      assert.equal(out.data, "hi");
      assert.equal(cf.status, FutureStatus.FINISHED);
      assert.equal(cf.stage_future_ids.length, 1);
    }));

  test("003 empty stage_fns raises", () => {
    const [, , io_id, policy_gid] = _ids();
    assert.throws(() => new ComplexFuture({ taskforce_id: io_id, policy_id: policy_gid, stage_fns: [] }), E.ValueError);
  });

  test("004 non callable stage raises", () => {
    const [, , io_id, policy_gid] = _ids();
    assert.throws(() => new ComplexFuture({ taskforce_id: io_id, policy_id: policy_gid, stage_fns: [(_) => null, "not_callable"] }), E.TypeError);
  });

  test("005 stage fn returning non future propagates error", () =>
    macrotask(() => {
      const [, , io_id, policy_gid] = _ids();
      const cf = new ComplexFuture({ taskforce_id: io_id, policy_id: policy_gid, stage_fns: [(_) => 123] });
      assert.throws(() => cf.wait(2));
      assert.equal(cf.status, FutureStatus.ERROR);
    }));

  test("006 first stage failure propagates", () =>
    macrotask(() => {
      const [cmd, compute_id, io_id, policy_gid] = _ids();
      const boom = () => {
        throw new E.ValueError("kaboom");
      };
      const s1 = (_) => cmd.submit([boom], { taskforce_id: compute_id });
      const cf = new ComplexFuture({ taskforce_id: io_id, policy_id: policy_gid, stage_fns: [s1] });
      assert.throws(() => cf.wait(5), E.ValueError);
      assert.equal(cf.status, FutureStatus.ERROR);
    }));

  test("007 second stage failure propagates", () =>
    macrotask(() => {
      const [cmd, compute_id, io_id, policy_gid] = _ids();
      const s1 = (_) => cmd.submit([() => 1], { taskforce_id: compute_id });
      const kaboom = (_) => {
        throw new E.RuntimeError("x");
      };
      const s2 = (prev) => cmd.submit([() => kaboom(prev)], { taskforce_id: compute_id });
      const cf = new ComplexFuture({ taskforce_id: io_id, policy_id: policy_gid, stage_fns: [s1, s2] });
      assert.throws(() => cf.wait(5), E.RuntimeError);
      assert.equal(cf.status, FutureStatus.ERROR);
    }));

  test("008 async stage runs on alpha taskforce", () =>
    macrotask(() => {
      const [cmd, compute_id, io_id, policy_gid] = _ids();
      const aiowork = async (x) => x + 100;
      const s1 = (_) => cmd.submit([() => 1], { taskforce_id: compute_id });
      const s2 = (prev) => {
        const v = prev.data;
        return cmd.submit([() => aiowork(v)], { taskforce_id: io_id });
      };
      const cf = new ComplexFuture({ taskforce_id: io_id, policy_id: policy_gid, stage_fns: [s1, s2] });
      assert.equal(cf.wait(5).data, 101);
    }));

  test("009 nested complex future as inner stage", () =>
    macrotask(() => {
      const [cmd, compute_id, io_id, policy_gid] = _ids();
      const s1 = (_) => cmd.submit([() => 5], { taskforce_id: compute_id });
      const s2 = (prev) => {
        const v = prev.data;
        return cmd.submit([() => v * 3], { taskforce_id: compute_id });
      };
      const inner = (_) => new ComplexFuture({ taskforce_id: io_id, policy_id: policy_gid, stage_fns: [s1, s2] });
      const outer_s2 = (prev) => {
        const v = prev.data;
        return cmd.submit([() => v + 7], { taskforce_id: compute_id });
      };
      const outer = new ComplexFuture({ taskforce_id: io_id, policy_id: policy_gid, stage_fns: [inner, outer_s2] });
      assert.equal(outer.wait(10).data, 22);
    }));

  test("010 branching in stage fn", () =>
    macrotask(() => {
      const [cmd, compute_id, io_id, policy_gid] = _ids();
      const s1 = (_) => cmd.submit([() => 4], { taskforce_id: compute_id });
      const branch = (prev) => {
        const v = prev.data;
        if (v % 2 === 0) return cmd.submit([() => v * 10], { taskforce_id: compute_id });
        return cmd.submit([() => v + 1], { taskforce_id: compute_id });
      };
      const cf = new ComplexFuture({ taskforce_id: io_id, policy_id: policy_gid, stage_fns: [s1, branch] });
      assert.equal(cf.wait(5).data, 40);
    }));

  test("011 registered in future bank once per layer", () =>
    macrotask(() => {
      const [cmd, compute_id, io_id, policy_gid] = _ids();
      const bank = LAILA.active_policy.future_bank;
      const before = dict_len(bank);
      const s1 = (_) => cmd.submit([() => 1], { taskforce_id: compute_id });
      const cf = new ComplexFuture({ taskforce_id: io_id, policy_id: policy_gid, stage_fns: [s1] });
      cf.wait(5);
      assert.ok(dict_has(bank, cf.global_id));
      assert.ok(dict_len(bank) > before);
    }));

  test("012 future identity roundtrips through bank", () =>
    macrotask(() => {
      const [cmd, compute_id, io_id, policy_gid] = _ids();
      const s1 = (_) => cmd.submit([() => "abc"], { taskforce_id: compute_id });
      const cf = new ComplexFuture({ taskforce_id: io_id, policy_id: policy_gid, stage_fns: [s1] });
      const ident = cf.future_identity;
      assert.equal(ident.wait(5).data, "abc");
    }));

  test("013 group future as stage aggregates", () =>
    macrotask(() => {
      const [cmd, compute_id, io_id, policy_gid] = _ids();
      const fanout = (_) => cmd.submit([1, 2, 3].map((v) => () => v * 2), { taskforce_id: compute_id });
      const cf = new ComplexFuture({ taskforce_id: io_id, policy_id: policy_gid, stage_fns: [fanout] });
      cf.wait(5);
      assert.equal(cf.status, FutureStatus.FINISHED);
    }));
});

// ---------------------------------------------------------------------------
// test_future_release_and_lazy_result.py
// ---------------------------------------------------------------------------
describe("TestFutureRelease", () => {
  test("001 single future release removes from bank", () =>
    macrotask(() => {
      const fut = LAILA.command.submit([() => 1]);
      const gid = fut.global_id;
      assert.ok(dict_has(_bank(), gid));
      fut.wait(_T);
      assert.ok(dict_has(_bank(), gid));
      void fut.data;
      assert.ok(dict_has(_bank(), gid));
      fut.release();
      assert.ok(!dict_has(_bank(), gid));
      fut.release();
      assert.ok(!dict_has(_bank(), gid));
      assert.equal(fut.data, 1);
    }));

  test("002 group release releases children by default", () =>
    macrotask(() => {
      const gf = LAILA.command.submit([() => 1, () => 2, () => 3]);
      const ids = [...gf.future_ids];
      gf.wait(_T);
      for (const fid of ids) assert.ok(dict_has(_bank(), fid));
      gf.release();
      assert.ok(!dict_has(_bank(), gf.global_id));
      for (const fid of ids) assert.ok(!dict_has(_bank(), fid));
      gf.release();
    }));

  test("003 group release children=False keeps children", () =>
    macrotask(() => {
      const gf = LAILA.command.submit([() => 1, () => 2]);
      const ids = [...gf.future_ids];
      gf.wait(_T);
      gf.release({ children: false });
      assert.ok(!dict_has(_bank(), gf.global_id));
      for (const fid of ids) {
        assert.ok(dict_has(_bank(), fid));
        getitem(_bank(), fid).release();
        assert.ok(!dict_has(_bank(), fid));
      }
    }));

  test("004 early release does not break completion", () =>
    macrotask(() => {
      const fut = LAILA.command.submit([() => time.sleep(0.2) || 42]);
      fut.release();
      assert.ok(!dict_has(_bank(), fut.global_id));
      assert.equal(fut.wait(_T).data, 42);
      assert.equal(fut.status, FutureStatus.FINISHED);
    }));

  test("005 identity handle release delegates to concrete future", () =>
    macrotask(() => {
      const fut = LAILA.command.submit([() => 5]);
      fut.wait(_T);
      const ident = fut.future_identity;
      assert.ok(ident instanceof _LAILA_IDENTIFIABLE_FUTURE);
      assert.ok(!(ident instanceof Future));
      assert.ok(dict_has(_bank(), fut.global_id));
      ident.release();
      assert.ok(!dict_has(_bank(), fut.global_id));
      ident.release();
    }));

  test("006 complex future release releases stages", () =>
    macrotask(() => {
      const cmd = LAILA.command;
      const alpha = cmd.alpha_taskforce;
      const pid = LAILA.active_policy.global_id;
      const s1 = (_) => cmd.submit([() => 7], { taskforce_id: alpha });
      const s2 = (prev) => {
        const v = prev.data;
        return cmd.submit([() => v * 6], { taskforce_id: alpha });
      };
      const cf = new ComplexFuture({ taskforce_id: alpha, policy_id: pid, stage_fns: [s1, s2] });
      assert.equal(cf.wait(_T).data, 42);
      const stage_ids = [...cf.stage_future_ids];
      assert.equal(stage_ids.length, 2);
      for (const sid of stage_ids) assert.ok(dict_has(_bank(), sid));
      cf.release();
      assert.ok(!dict_has(_bank(), cf.global_id));
      for (const sid of stage_ids) assert.ok(!dict_has(_bank(), sid));
    }));
});

describe("TestLazyResultWrap", () => {
  test("001 raw result is not wrapped until read", () =>
    macrotask(() => {
      const fut = LAILA.command.submit([() => ({ x: 1 })]);
      const done = new TH.Event();
      fut.add_status_callback(FutureStatus.FINISHED, (_f) => done.set());
      assert.ok(done.wait(_T));
      with_(fut.atomic(), () => {
        assert.ok(fut._result_pending_wrap);
        assert.equal(fut._result_global_id, null);
        assert.deepEqual(fut._return_value, { x: 1 });
      });
      const res = fut.result;
      assert.ok(res instanceof Entry);
      assert.deepEqual(res.data, { x: 1 });
      assert.ok(!fut._result_pending_wrap);
      assert.equal(fut.result_global_id, res.global_id);
      assert.equal(fut.result, res);
      assert.deepEqual(fut.data, { x: 1 });
      fut.release();
    }));

  test("002 wait and await return entries", async () => {
    await macrotask(() => {
      const fut = LAILA.command.submit([() => 3]);
      const out = fut.wait(_T);
      assert.ok(out instanceof Entry);
      assert.equal(out.data, 3);
      fut.release();
    });

    const _go = async () => {
      const f = LAILA.command.submit([() => 4]);
      try {
        return await f;
      } finally {
        f.release();
      }
    };
    const res = await _go();
    assert.ok(res instanceof Entry);
    assert.equal(res.data, 4);
  });

  test("003 entry results are stored as is", () =>
    macrotask(() => {
      const e = Entry.constant(9);
      const fut = LAILA.command.submit([() => e]);
      const out = fut.wait(_T);
      assert.equal(out, e);
      assert.ok(!fut._result_pending_wrap);
      assert.equal(fut.result_global_id, e.global_id);
      fut.release();
    }));

  test("004 rpc helpers force wrap", () =>
    macrotask(() => {
      const policy = LAILA.get_active_policy();
      const fut = LAILA.command.submit([() => "abc"]);
      fut.wait(_T);
      const rid = policy._get_future_result_id(fut.global_id);
      assert.notEqual(rid, null);
      assert.equal(rid, fut.result.global_id);
      const gf = LAILA.command.submit([() => 1, () => 2]);
      gf.wait(_T);
      const ids = policy._get_future_result_id(gf.global_id);
      assert.equal(ids.length, 2);
      assert.ok(ids.every((i) => i !== null));
      fut.release();
      gf.release();
    }));

  test("005 complex future chain sees entries", () =>
    macrotask(() => {
      const cmd = LAILA.command;
      const alpha = cmd.alpha_taskforce;
      const pid = LAILA.active_policy.global_id;
      const s1 = (_) => cmd.submit([() => 7], { taskforce_id: alpha });
      const s2 = (prev) => {
        const v = prev.data;
        return cmd.submit([() => v + 1], { taskforce_id: alpha });
      };
      const cf = new ComplexFuture({ taskforce_id: alpha, policy_id: pid, stage_fns: [s1, s2] });
      const out = cf.wait(_T);
      assert.ok(out instanceof Entry);
      assert.equal(out.data, 8);
      cf.release();
    }));

  test("006 None result has no id", () =>
    macrotask(() => {
      const fut = LAILA.command.submit([() => null]);
      fut.wait(_T);
      assert.equal(fut.result_global_id, null);
      assert.equal(fut.result, null);
      fut.release();
    }));
});

describe("TestSubmitReturnTypeAndIdentity", () => {
  test("001 single task submit returns concrete future", () =>
    macrotask(() => {
      const fut = LAILA.command.submit([() => 1]);
      assert.ok(fut instanceof ConcurrentPackageFuture);
      assert.ok(fut instanceof _LAILA_IDENTIFIABLE_FUTURE);
      assert.equal(getitem(_bank(), fut.global_id), fut);
      assert.equal(fut.wait(_T).data, 1);
      fut.release();
    }));

  test("002 imap yields concrete futures", () =>
    macrotask(() => {
      const tf = getitem(LAILA.command.taskforces, LAILA.command.alpha_taskforce);
      const futs = [...tf.imap([() => 1, () => 2])];
      assert.ok(futs.every((f) => f instanceof ConcurrentPackageFuture));
      assert.deepEqual(
        futs.map((f) => f.wait(_T).data),
        [1, 2],
      );
      for (const f of futs) f.release();
    }));

  test("004 explicit uuid is honoured on futures", () => {
    const u = crypto.randomUUID();
    const pid = LAILA.active_policy.global_id;
    const tf = LAILA.command.alpha_taskforce;
    const f = new ConcurrentPackageFuture({ taskforce_id: tf, policy_id: pid, uuid: u });
    assert.equal(f.uuid, u);
    assert.ok(f.global_id.endsWith(u));
    assert.ok(dict_has(_bank(), f.global_id));
    const g = new GroupFuture({ taskforce_id: tf, policy_id: pid, uuid: u, future_ids: [] });
    assert.equal(g.uuid, u);
    assert.notEqual(f.global_id, g.global_id);
    assert.deepEqual(f.scopes, ["FUTURE"]);
    assert.deepEqual(g.scopes, ["GROUP_FUTURE"]);
    f.release();
    g.release();
  });

  test("005 default scopes per class", () => {
    const e = Entry.constant(1);
    assert.deepEqual(e.scopes, ["ENTRY"]);
    const custom = new Entry({ data: 1, scopes: ["X", "Y"] });
    assert.deepEqual(custom.scopes, ["X", "Y"]);
    const p = LAILA.get_active_policy();
    assert.deepEqual(p.scopes, ["POLICY"]);
    assert.deepEqual(p.central.command.scopes, ["CENTRAL_COMMAND"]);
  });

  test("006 default callbacks shared and status callbacks per instance", () => {
    const pid = LAILA.active_policy.global_id;
    const tf = LAILA.command.alpha_taskforce;
    const a = new ConcurrentPackageFuture({ taskforce_id: tf, policy_id: pid });
    const b = new ConcurrentPackageFuture({ taskforce_id: tf, policy_id: pid });
    assert.equal(a._default_callbacks, b._default_callbacks);
    assert.notEqual(a._status_callbacks, b._status_callbacks);
    getitem(a._default_callbacks, FutureStatus.FINISHED)(a);
    assert.equal(a.status, FutureStatus.FINISHED);
    assert.equal(b.status, FutureStatus.NOT_STARTED);
    a.release();
    b.release();
  });
});

// ---------------------------------------------------------------------------
// test_python_async_thread_pool_task_force.py
// ---------------------------------------------------------------------------
function _new_tf({ num_workers = 2, max_async_per_thread = 8, sync_workers = null, rank = null } = {}) {
  const kwargs = { policy_id: LAILA.active_policy.global_id, num_workers, max_async_per_thread };
  if (sync_workers !== null) kwargs.sync_workers = sync_workers;
  if (rank !== null) kwargs.rank = rank;
  const tf = new PythonAsyncThreadPoolTaskForce(kwargs);
  tf.start();
  LAILA.active_policy.central.command.taskforces[tf.global_id] = tf;
  return tf;
}

function _drop_tf(tf, kw = {}) {
  try {
    tf.shutdown(kw);
  } finally {
    delete LAILA.active_policy.central.command.taskforces[tf.global_id];
  }
}

describe("TestPythonAsyncThreadPoolTaskForce", () => {
  test("001 sync task runs", () =>
    macrotask(() => {
      const tf = _new_tf();
      try {
        const ident = tf.submit([() => 7]);
        assert.equal(ident.wait(5).data, 7);
      } finally {
        tf.shutdown();
      }
    }));

  test("002 async task runs", () =>
    macrotask(() => {
      const tf = _new_tf();
      try {
        const aiowork = async () => {
          await asyncio.sleep(0.01);
          return "ok";
        };
        const ident = tf.submit([() => aiowork()]);
        assert.equal(ident.wait(5).data, "ok");
      } finally {
        tf.shutdown();
      }
    }));

  test("003 mixed batch returns group future", () =>
    macrotask(() => {
      const tf = _new_tf();
      try {
        const a = async () => {
          await asyncio.sleep(0.005);
          return 1;
        };
        const gf = tf.submit([() => a(), () => 2, () => a()]);
        assert.ok(gf instanceof GroupFuture);
        const outs = gf.wait(5);
        const data = outs.map(_unwrap);
        assert.deepEqual([...data].sort(), [1, 1, 2]);
      } finally {
        tf.shutdown();
      }
    }));

  test("004 max_async_per_thread caps concurrency", () =>
    macrotask(() => {
      const cap = 3;
      const tf = _new_tf({ num_workers: 1, max_async_per_thread: cap });
      try {
        const inflight_peak = { v: 0 };
        const inflight_now = { v: 0 };
        const slow = async () => {
          inflight_now.v += 1;
          inflight_peak.v = Math.max(inflight_peak.v, inflight_now.v);
          await asyncio.sleep(0.05);
          inflight_now.v -= 1;
          return 1;
        };
        tf.submit(T.range(20).map(() => () => slow())).wait(20);
        assert.ok(inflight_peak.v <= cap);
      } finally {
        tf.shutdown();
      }
    }));

  test("005 async task exception records error", () =>
    macrotask(() => {
      const tf = _new_tf();
      try {
        const boom = async () => {
          throw new E.ValueError("nope");
        };
        const ident = tf.submit([() => boom()]);
        assert.throws(() => ident.wait(5), E.ValueError);
        assert.equal(ident.status, FutureStatus.ERROR);
      } finally {
        tf.shutdown();
      }
    }));

  test("006 sync task exception records error", () =>
    macrotask(() => {
      const tf = _new_tf();
      try {
        const boom = () => {
          throw new E.RuntimeError("xx");
        };
        const ident = tf.submit([boom]);
        assert.throws(() => ident.wait(5), E.RuntimeError);
      } finally {
        tf.shutdown();
      }
    }));

  test("007 two workers actually parallel", () =>
    macrotask(() => {
      const tf = _new_tf({ num_workers: 2, max_async_per_thread: 1 });
      try {
        const slow = async () => {
          await asyncio.sleep(0.2);
          return 1;
        };
        const t0 = time.monotonic();
        tf.submit([() => slow(), () => slow()]).wait(5);
        assert.ok(time.monotonic() - t0 < 0.35);
      } finally {
        tf.shutdown();
      }
    }));

  test("008 shutdown cancels pending", () =>
    macrotask(() => {
      const tf = _new_tf({ num_workers: 1, max_async_per_thread: 1 });
      try {
        const hold = async () => {
          await asyncio.sleep(2.0);
        };
        tf.submit([() => hold()]);
        const queued = tf.submit([() => 1]);
        time.sleep(0.05);
        tf.shutdown({ wait: true, cancel_pending: true });
        assert.equal(queued.status, FutureStatus.CANCELLED);
      } catch (e) {
        try {
          tf.shutdown();
        } catch {
          /* ignore */
        }
        throw e;
      }
    }));

  test("009 single future bank", () =>
    macrotask(() => {
      const tf = _new_tf();
      try {
        const ident = tf.submit([() => 99]);
        ident.wait(5);
        assert.ok(dict_has(LAILA.active_policy.future_bank, ident.global_id));
      } finally {
        tf.shutdown();
      }
    }));
});

// ---------------------------------------------------------------------------
// test_parking.py
// ---------------------------------------------------------------------------
describe("TestNestedSubmissionsDoNotDeadlock", () => {
  const cmd = () => LAILA.command;
  const tiny = (opts = {}) => _new_tf({ num_workers: 1, max_async_per_thread: 1, ...opts });

  test("001 async parent awaits child on 1x1", () =>
    macrotask(() => {
      const tf = tiny();
      try {
        const outer = async () => {
          const inner = cmd().submit([() => 41], { taskforce_id: tf.global_id });
          return _unwrap(await inner) + 1;
        };
        const fut = cmd().submit([outer], { taskforce_id: tf.global_id });
        assert.equal(_unwrap(fut.wait(_T)), 42);
      } finally {
        _drop_tf(tf);
      }
    }));

  test("002 sync parent blocks on child on 1x1", () =>
    macrotask(() => {
      const tf = tiny();
      try {
        const outer = () => {
          const inner = cmd().submit([() => "child"], { taskforce_id: tf.global_id });
          return _unwrap(inner.wait(_T)) + "-parent";
        };
        const fut = cmd().submit([outer], { taskforce_id: tf.global_id });
        assert.equal(_unwrap(fut.wait(_T)), "child-parent");
      } finally {
        _drop_tf(tf);
      }
    }));

  test("003 deep recursion on 1x1", () =>
    macrotask(() => {
      const tf = tiny();
      try {
        const rec = async (n) => {
          if (n === 0) return 0;
          const child = cmd().submit([partial(rec, n - 1)], { taskforce_id: tf.global_id });
          return 1 + _unwrap(await child);
        };
        const fut = cmd().submit([partial(rec, 60)], { taskforce_id: tf.global_id });
        assert.equal(_unwrap(fut.wait(_T)), 60);
        assert.equal(tf.inflight, 0);
        assert.equal(tf.parked, 0);
      } finally {
        _drop_tf(tf);
      }
    }));

  test("004 wide fanout under saturation respects cap", () =>
    macrotask(() => {
      const cap = 2;
      const tf = _new_tf({ num_workers: 2, max_async_per_thread: cap });
      try {
        const running = { now: 0, peak: 0 };
        const _enter = () => {
          running.now += 1;
          running.peak = Math.max(running.peak, running.now);
        };
        const _leave = () => {
          running.now -= 1;
        };
        const leaf = async (i) => {
          _enter();
          await asyncio.sleep(0.001);
          _leave();
          return i;
        };
        const parent = async (base) => {
          _enter();
          const gf = cmd().submit(
            T.range(5).map((k) => partial(leaf, base + k)),
            { taskforce_id: tf.global_id },
          );
          _leave();
          const results = await gf;
          _enter();
          const total = results.reduce((acc, r) => acc + _unwrap(r), 0);
          _leave();
          return total;
        };
        const n_parents = 120;
        const gf = cmd().submit(
          T.range(n_parents).map((p) => partial(parent, 10 * p)),
          { taskforce_id: tf.global_id },
        );
        const outs = gf.wait(_T).map(_unwrap);
        const expected = T.range(n_parents).map((p) => T.range(5).reduce((acc, k) => acc + 10 * p + k, 0));
        assert.deepEqual(outs, expected);
        assert.ok(running.peak <= 2 * cap, `peak ${running.peak}`);
        assert.equal(tf.inflight, 0);
        assert.equal(tf.parked, 0);
      } finally {
        _drop_tf(tf);
      }
    }));

  test("005 group future awaited inside slot on 1x1", () =>
    macrotask(() => {
      const tf = tiny();
      try {
        const outer = async () => {
          const gf = cmd().submit([() => 1, () => 2, () => 3], { taskforce_id: tf.global_id });
          return (await gf).reduce((acc, r) => acc + _unwrap(r), 0);
        };
        const fut = cmd().submit([outer], { taskforce_id: tf.global_id });
        assert.equal(_unwrap(fut.wait(_T)), 6);
      } finally {
        _drop_tf(tf);
      }
    }));

  test("006 sync parent group wait inside slot on 1x1", () =>
    macrotask(() => {
      const tf = tiny();
      try {
        const outer = () => {
          const gf = cmd().submit([() => 1, () => 2, () => 3], { taskforce_id: tf.global_id });
          return gf.wait(_T).reduce((acc, r) => acc + _unwrap(r), 0);
        };
        const fut = cmd().submit([outer], { taskforce_id: tf.global_id });
        assert.equal(_unwrap(fut.wait(_T)), 6);
      } finally {
        _drop_tf(tf);
      }
    }));

  test("008 child error propagates and slot is restored", () =>
    macrotask(() => {
      const tf = tiny();
      try {
        const bad_child = async () => {
          throw new E.ValueError("child failed");
        };
        const outer = async () => {
          const child = cmd().submit([bad_child], { taskforce_id: tf.global_id });
          try {
            await child;
          } catch (exc) {
            if (exc instanceof E.ValueError) return `caught:${exc.message}`;
            throw exc;
          }
          return "not raised";
        };
        const fut = cmd().submit([outer], { taskforce_id: tf.global_id });
        assert.equal(_unwrap(fut.wait(_T)), "caught:child failed");
        const again = cmd().submit([() => 5], { taskforce_id: tf.global_id });
        assert.equal(_unwrap(again.wait(_T)), 5);
        assert.equal(tf.inflight, 0);
      } finally {
        _drop_tf(tf);
      }
    }));

  test("009 parked parent resumes before new roots", () =>
    macrotask(() => {
      const tf = tiny();
      try {
        const order = [];
        const log = (tag) => order.push(tag);
        const child = async () => {
          log("c");
          await asyncio.sleep(0.1);
          return 1;
        };
        const a = async () => {
          log("A-start");
          const ch = cmd().submit([child], { taskforce_id: tf.global_id });
          await ch;
          log("A-resume");
          return "A";
        };
        const b = async () => {
          log("B");
          await asyncio.sleep(0.4);
          return "B";
        };
        const d = async () => {
          log("D");
          return "D";
        };
        const fa = cmd().submit([a], { taskforce_id: tf.global_id });
        time.sleep(0.05);
        const fb = cmd().submit([b], { taskforce_id: tf.global_id });
        time.sleep(0.2);
        const fd = cmd().submit([d], { taskforce_id: tf.global_id });
        for (const f of [fa, fb, fd]) f.wait(_T);
        assert.deepEqual(order.slice(0, 2), ["A-start", "c"]);
        assert.ok(order.indexOf("A-resume") < order.indexOf("D"), order.join(","));
      } finally {
        _drop_tf(tf);
      }
    }));
});

describe("TestSyncOffloadAndPermits", () => {
  const cmd = () => LAILA.command;

  test("010 sync body does not block loop", () =>
    macrotask(() => {
      const tf = _new_tf({ num_workers: 1, max_async_per_thread: 4, sync_workers: 4 });
      try {
        const sleepy = () => {
          time.sleep(0.4);
          return "slept";
        };
        const quick = async () => "quick";
        const slow = cmd().submit([sleepy, sleepy, sleepy], { taskforce_id: tf.global_id });
        const t0 = time.monotonic();
        const q = cmd().submit([quick], { taskforce_id: tf.global_id });
        // Node mapping: a blocking ``time.sleep`` in a sync body pumps the
        // one real thread, so the *caller's* ``wait`` can only return once
        // every nested pump has unwound. The loop itself is not blocked:
        // ``quick`` completes well inside the sleeps, which is what the
        // Python assertion measures -- timestamp its completion instead.
        const finished_at = { v: null };
        q.add_status_callback(FutureStatus.FINISHED, (_f) => {
          finished_at.v = time.monotonic();
        });
        assert.equal(_unwrap(q.wait(_T)), "quick");
        assert.ok(finished_at.v !== null && finished_at.v - t0 < 0.3, `quick finished after ${finished_at.v - t0}s`);
        assert.deepEqual(slow.wait(_T).map(_unwrap), ["slept", "slept", "slept"]);
      } finally {
        _drop_tf(tf);
      }
    }));

  test("011 sync_workers bounds executing bodies", () =>
    macrotask(() => {
      const tf = _new_tf({ num_workers: 1, max_async_per_thread: 8, sync_workers: 2 });
      try {
        const state = { now: 0, peak: 0 };
        const body = () => {
          state.now += 1;
          state.peak = Math.max(state.peak, state.now);
          time.sleep(0.05);
          state.now -= 1;
          return 1;
        };
        cmd()
          .submit(
            T.range(8).map(() => body),
            { taskforce_id: tf.global_id },
          )
          .wait(_T);
        assert.ok(state.peak <= 2, `peak ${state.peak}`);
      } finally {
        _drop_tf(tf);
      }
    }));

  test("012 permit released while blocked on child", () =>
    macrotask(() => {
      const tf = _new_tf({ num_workers: 1, max_async_per_thread: 1, sync_workers: 1 });
      try {
        const leaf = () => 1;
        const mid = () => {
          const f = cmd().submit([leaf], { taskforce_id: tf.global_id });
          return _unwrap(f.wait(_T)) + 1;
        };
        const top = () => {
          const f = cmd().submit([mid], { taskforce_id: tf.global_id });
          return _unwrap(f.wait(_T)) + 1;
        };
        const fut = cmd().submit([top], { taskforce_id: tf.global_id });
        assert.equal(_unwrap(fut.wait(_T)), 3);
      } finally {
        _drop_tf(tf);
      }
    }));

  test("013 slot context visible inside sync body", () =>
    macrotask(() => {
      const tf = _new_tf({ num_workers: 1, max_async_per_thread: 1 });
      try {
        const body = () => {
          const ctx = _CURRENT_SLOT.get();
          return ctx !== null && ctx.tf === tf;
        };
        const fut = cmd().submit([body], { taskforce_id: tf.global_id });
        assert.equal(_unwrap(fut.wait(_T)), true);
        assert.equal(_CURRENT_SLOT.get(), null);
      } finally {
        _drop_tf(tf);
      }
    }));

  test("014 lambda returning coroutine still works", () =>
    macrotask(() => {
      const tf = _new_tf({ num_workers: 1, max_async_per_thread: 1 });
      try {
        const inner = async () => {
          await asyncio.sleep(0.001);
          return "coro";
        };
        const fut = cmd().submit([() => inner()], { taskforce_id: tf.global_id });
        assert.equal(_unwrap(fut.wait(_T)), "coro");
      } finally {
        _drop_tf(tf);
      }
    }));
});

describe("TestShutdownAndCancellation", () => {
  const cmd = () => LAILA.command;

  test("015 running task marked cancelled on shutdown", () =>
    macrotask(() => {
      const tf = _new_tf({ num_workers: 1, max_async_per_thread: 1 });
      try {
        const hold = async () => {
          await asyncio.sleep(30);
        };
        const fut = cmd().submit([hold], { taskforce_id: tf.global_id });
        time.sleep(0.1);
        assert.equal(fut.status, FutureStatus.RUNNING);
        _drop_tf(tf, { wait: true, cancel_pending: true });
        assert.equal(fut.status, FutureStatus.CANCELLED);
        assert.throws(() => fut.wait(1), E.RuntimeError);
      } finally {
        _drop_tf(tf);
      }
    }));

  test("016 parked task does not hang shutdown", () =>
    macrotask(() => {
      const tf = _new_tf({ num_workers: 1, max_async_per_thread: 1 });
      try {
        const child = async () => {
          await asyncio.sleep(30);
        };
        const parent = async () => {
          await cmd().submit([child], { taskforce_id: tf.global_id });
        };
        const fut = cmd().submit([parent], { taskforce_id: tf.global_id });
        time.sleep(0.2);
        const t0 = time.monotonic();
        _drop_tf(tf, { wait: true, cancel_pending: true });
        assert.ok(time.monotonic() - t0 < 5.0);
        assert.ok([FutureStatus.CANCELLED, FutureStatus.ERROR].includes(fut.status));
      } finally {
        _drop_tf(tf);
      }
    }));
});

describe("TestTaskforceSplitAndRank", () => {
  const cmd = () => LAILA.command;

  test("017 default command has alpha and internal", () => {
    const c = cmd();
    assert.notEqual(c.alpha_taskforce, null);
    assert.notEqual(c.internal_taskforce, null);
    assert.notEqual(c.alpha_taskforce, c.internal_taskforce);
    assert.equal(getitem(c.taskforces, c.alpha_taskforce).rank, 2);
    assert.equal(getitem(c.taskforces, c.internal_taskforce).rank, 1);
  });

  test("018 alpha task may submit to internal", () =>
    macrotask(() => {
      const alpha = _new_tf({ num_workers: 1, max_async_per_thread: 1, rank: 2 });
      const internal = _new_tf({ num_workers: 1, max_async_per_thread: 1, rank: 1 });
      try {
        const outer = async () => {
          const f = cmd().submit([() => "ok"], { taskforce_id: internal.global_id });
          return _unwrap(await f);
        };
        const fut = cmd().submit([outer], { taskforce_id: alpha.global_id });
        assert.equal(_unwrap(fut.wait(_T)), "ok");
      } finally {
        _drop_tf(alpha);
        _drop_tf(internal);
      }
    }));

  test("019 internal task may not submit to alpha", () =>
    macrotask(() => {
      const alpha = _new_tf({ num_workers: 1, max_async_per_thread: 1, rank: 2 });
      const internal = _new_tf({ num_workers: 1, max_async_per_thread: 1, rank: 1 });
      try {
        const outer = async () => await cmd().submit([() => "nope"], { taskforce_id: alpha.global_id });
        const fut = cmd().submit([outer], { taskforce_id: internal.global_id });
        assert.throws(() => fut.wait(_T), NestedCommandSubmitError);
      } finally {
        _drop_tf(alpha);
        _drop_tf(internal);
      }
    }));

  test("020 same rank and same taskforce allowed", () =>
    macrotask(() => {
      const a = _new_tf({ num_workers: 1, max_async_per_thread: 1, rank: 2 });
      const b = _new_tf({ num_workers: 1, max_async_per_thread: 1, rank: 2 });
      try {
        const outer = async () => {
          const x = _unwrap(await cmd().submit([() => 1], { taskforce_id: a.global_id }));
          const y = _unwrap(await cmd().submit([() => 2], { taskforce_id: b.global_id }));
          return x + y;
        };
        const fut = cmd().submit([outer], { taskforce_id: a.global_id });
        assert.equal(_unwrap(fut.wait(_T)), 3);
      } finally {
        _drop_tf(a);
        _drop_tf(b);
      }
    }));

  test("021 user thread unrestricted", () =>
    macrotask(() => {
      const fut = cmd().submit([() => 9], { taskforce_id: cmd().internal_taskforce });
      assert.equal(_unwrap(fut.wait(_T)), 9);
    }));
});

describe("TestCycleGuard", () => {
  test("023 resolve chain guard raises on active chain", () => {
    // ``laila.remember`` is p6; exercise the guard it uses directly.
    const { check_resolve_cycle } = CMD;
    const gid = "LAILA:ENTRY:x";
    const token = _RESOLVE_CHAIN.set(Object.freeze([gid]));
    try {
      assert.throws(() => check_resolve_cycle(gid), CyclicDependencyError);
    } finally {
      _RESOLVE_CHAIN.reset(token);
    }
    check_resolve_cycle(gid);
  });

  test("024 chain propagates into child root tasks", () =>
    macrotask(() => {
      const cmd = LAILA.command;
      const child = async () => _RESOLVE_CHAIN.get();
      const parent = async () => {
        const token = _RESOLVE_CHAIN.set(Object.freeze([..._RESOLVE_CHAIN.get(), "LAILA:ENTRY:p"]));
        try {
          return _unwrap(await cmd.submit([child]));
        } finally {
          _RESOLVE_CHAIN.reset(token);
        }
      };
      const chain = _unwrap(cmd.submit([parent]).wait(_T));
      assert.deepEqual([...chain], ["LAILA:ENTRY:p"]);
    }));
});

// ---------------------------------------------------------------------------
// test_python_process_pool_task_force.py
// ---------------------------------------------------------------------------
describe("TestPythonProcessPoolTaskForce", () => {
  const _make_force = (num_workers = 4) => new PythonProcessPoolTaskForce({ policy_id: "LAILA:POLICY:test-policy", num_workers });

  test("001 start and shutdown", () =>
    macrotask(() => {
      const tf = _make_force();
      tf.start();
      assert.equal(tf.status.value, "running");
      tf.shutdown({ wait: true });
      assert.equal(tf.status.value, "stopped");
    }));

  test("002 submit single task returns future identity", () =>
    macrotask(() => {
      const tf = _make_force();
      tf.start();
      try {
        const identity = tf.submit([PP._return_7], { wait: false });
        assert.ok(identity instanceof _LAILA_IDENTIFIABLE_FUTURE);
        const fut = getitem(LAILA.get_active_policy().future_bank, identity.global_id);
        assert.ok(fut instanceof ProcessPackageFuture);
        assert.equal(fut.wait(), 7);
        assert.equal(identity.status, FutureStatus.FINISHED);
      } finally {
        tf.shutdown({ wait: true });
      }
    }));

  test("003 submit multiple tasks returns group future", () =>
    macrotask(() => {
      const tf = _make_force();
      tf.start();
      try {
        const out = tf.submit([PP._return_1, PP._return_2, PP._return_3], { wait: false });
        assert.ok(out instanceof GroupFuture);
        assert.deepEqual(out.wait(), [1, 2, 3]);
        assert.equal(Number(out.status.percentages.finished), 100.0);
      } finally {
        tf.shutdown({ wait: true });
      }
    }));

  test("004 submit wait=True returns values", () =>
    macrotask(() => {
      const tf = _make_force();
      tf.start();
      try {
        assert.deepEqual(tf.submit([PP._return_a, PP._return_b], { wait: true }), ["a", "b"]);
      } finally {
        tf.shutdown({ wait: true });
      }
    }));

  test("005 imap yields identities in order", () =>
    macrotask(() => {
      const tf = _make_force();
      tf.start();
      try {
        const identities = [...tf.imap([PP._return_10, PP._return_20])];
        assert.equal(identities.length, 2);
        const bank = LAILA.get_active_policy().future_bank;
        assert.deepEqual(
          identities.map((i) => getitem(bank, i.global_id).wait()),
          [10, 20],
        );
      } finally {
        tf.shutdown({ wait: true });
      }
    }));

  test("006 queue_submit allows submission on auto-started force", () =>
    macrotask(() => {
      const tf = _make_force();
      try {
        const fut = tf._queue_submit(PP._return_1);
        assert.equal(fut.wait(), 1);
      } finally {
        tf.shutdown({ wait: true });
      }
    }));

  test("007 submit after shutdown raises", () =>
    macrotask(() => {
      const tf = _make_force();
      tf.start();
      tf.shutdown({ wait: true });
      assert.throws(() => tf.submit([PP._return_1], { wait: false }), E.RuntimeError);
    }));

  test("008 lambda task raises TypeError", () =>
    macrotask(() => {
      const tf = _make_force();
      try {
        assert.throws(() => tf.submit([() => 1], { wait: false }), E.TypeError);
      } finally {
        tf.shutdown({ wait: true });
      }
    }));
});

// Tear down the lazily-activated default policy so the process exits.
test("zz teardown", () =>
  macrotask(() => {
    LAILA.get_active_policy().central.command.shutdown({ wait: true, cancel_pending: true });
  }));
