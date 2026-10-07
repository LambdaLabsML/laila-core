/**
 * Atomic types: ports of
 *   tests/functional/atomics/unit_tests/test_atomic_dict_concurrency.py
 *   tests/functional/atomics/unit_tests/test_atomic_dict_functional.py
 *   tests/functional/atomics/unit_tests/test_atomic_dotmap.py
 *   tests/functional/atomics/unit_tests/test_atomic_flag.py
 *   tests/functional/atomics/unit_tests/test_atomic_int.py
 *   tests/functional/atomics/unit_tests/test_atomic_list.py
 *   tests/functional/atomics/unit_tests/test_atomic_str.py
 *
 * Python threads are the cooperative ``Thread`` of ``_compat/threading.js``:
 * a body runs to completion unless it blocks (lock contention, ``sleep``,
 * ``join``), and a blocking wait pumps the event loop so *other* threads run
 * underneath it. Thread counts / iteration counts are the Python ones.
 */
import { test, describe } from "node:test";
import assert from "node:assert/strict";

const S = new URL("../../src/", import.meta.url).href;
const T = await import(S + "_compat/pytypes.js");
const E = await import(S + "_compat/errors.js");
const TH = await import(S + "_compat/threading.js");
const PU = await import(S + "_compat/pump.js");
const time = await import(S + "_compat/time.js");
const { repr } = await import(S + "_compat/pyrepr.js");
const { with_ } = await import(S + "_compat/contextlib.js");
const { AtomicDict, AtomicDotMap, AtomicFlag, AtomicInt, AtomicList, AtomicStr } = await import(S + "atomic/index.js");

/**
 * Run a synchronous body on a fresh macrotask: ``node:test`` invokes test
 * bodies from a microtask, where a blocking wait (``Thread.join``) is
 * impossible by construction (nothing can settle until the job returns).
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

/** ``test`` whose synchronous body may block (threads, joins, sleeps). */
const t = (name, fn) => test(name, () => macrotask(fn));

/** ``[...tuples]`` -> plain nested arrays so ``assert.deepEqual`` can compare them. */
const pairs = (items) => items.map((p) => [...p]);

/** ``threading.BrokenBarrierError`` */
class BrokenBarrierError extends E.RuntimeError {}

/**
 * ``threading.Barrier(parties)`` for the cooperative thread model.
 *
 * ``wait()`` blocks (pumps the loop) until *parties* threads have arrived or
 * *timeout* seconds pass; a timeout breaks the barrier for everyone, exactly
 * like Python. Threads started in the same synchronous burst as the waiter
 * are still queued behind it, so by default ``wait`` first yields to them
 * (``yield_pending``) -- otherwise a rendezvous could never happen. Pass
 * ``yield_pending: false`` when the other parties would immediately block on
 * a lock the waiter holds: a thread blocked *above* the waiter on the stack
 * can never let the waiter's timeout unwind, so the only schedule the
 * cooperative runtime can realise is "waiter times out first" (which is also
 * what Python observes in that situation, see ``test_046``).
 */
class Barrier {
  constructor(parties) {
    this.parties = parties;
    this._count = 0;
    this._generation = 0;
    this.broken = false;
  }

  wait(timeout = null, { yield_pending = true } = {}) {
    if (this.broken) throw new BrokenBarrierError("barrier is broken");
    const gen = this._generation;
    this._count += 1;
    const index = this._count - 1;
    if (this._count === this.parties) {
      this._count = 0;
      this._generation += 1;
      return index;
    }
    if (yield_pending) PU.hop(() => {});
    const ok = TH.blocking_wait(() => this._generation !== gen || this.broken, timeout, { allow_locked: true });
    if (!ok || this.broken) {
      this.broken = true;
      throw new BrokenBarrierError(ok ? "barrier is broken" : "barrier wait timed out");
    }
    return index;
  }
}

// ── test_atomic_dict_concurrency.py ──────────────────────────────────────

describe("TestAtomicDictConcurrency", () => {
  const N_THREADS = 6;
  const N_ITERS = 200;

  /**
   * Spawn N threads, join them, and return the list.
   * If ``args_factory`` is null and ``target`` takes >=1 param, auto-pass ``(i,)``.
   */
  function _spawn(target, n = null, args_factory = null) {
    n = n || N_THREADS;
    const takes_index = args_factory === null && target.length >= 1;
    const threads = [];
    for (let i = 0; i < n; i++) {
      let args;
      if (args_factory !== null) args = args_factory(i);
      else if (takes_index) args = [i];
      else args = [];
      const th = new TH.Thread({ target, args });
      th.start();
      threads.push(th);
    }
    for (const th of threads) th.join();
    // A Python thread exception only prints a traceback; surface it here so
    // an assertion inside a worker fails the test.
    for (const th of threads) if (th.exception) throw th.exception;
    return threads;
  }

  t("test_001_concurrent_increments", () => {
    const d = new AtomicDict();
    function worker() {
      for (let k = 0; k < N_ITERS; k++) {
        with_(d.atomic("inc"), () => {
          const dd = AtomicDict.current();
          dd["cnt"] = dd.get("cnt", 0) + 1;
        });
      }
    }
    _spawn(worker);
    assert.equal(d["cnt"], N_THREADS * N_ITERS);
  });

  t("test_002_nested_atomic", () => {
    const d = new AtomicDict();
    function worker() {
      with_(d.atomic("outer"), () => {
        const dd = AtomicDict.current();
        dd["v"] = dd.get("v", 0) + 1;
        with_(d.atomic("inner"), () => {
          AtomicDict.current()["v"] += 1;
        });
      });
    }
    _spawn(worker);
    assert.equal(d["v"], N_THREADS * 2);
  });

  t("test_003_cross_instance_nonblocking", () => {
    const d1 = new AtomicDict();
    const d2 = new AtomicDict();
    const barrier = new Barrier(2);
    const results = [];
    function t1() {
      with_(d1.atomic("t1"), () => {
        barrier.wait();
        d1["a"] = 1;
        results.push("t1");
      });
    }
    function t2() {
      with_(d2.atomic("t2"), () => {
        barrier.wait();
        d2["b"] = 2;
        results.push("t2");
      });
    }
    const th1 = new TH.Thread({ target: t1 });
    const th2 = new TH.Thread({ target: t2 });
    th1.start();
    th2.start();
    th1.join();
    th2.join();
    for (const th of [th1, th2]) if (th.exception) throw th.exception;
    assert.deepEqual(T.sorted(results), ["t1", "t2"]);
    assert.equal(d1["a"], 1);
    assert.equal(d2["b"], 2);
  });

  t("test_004_unique_keys", () => {
    const d = new AtomicDict();
    function worker(i) {
      for (let k = 0; k < N_ITERS; k++) {
        with_(d.atomic(`t${i}`), () => {
          const dd = AtomicDict.current();
          dd[`k${i}`] = dd.get(`k${i}`, 0) + 1;
        });
      }
    }
    _spawn(worker);
    for (let i = 0; i < N_THREADS; i++) assert.equal(d[`k${i}`], N_ITERS);
  });

  t("test_005_exception_releases_lock", () => {
    const d = new AtomicDict();
    const started = [];
    function bad() {
      try {
        with_(d.atomic("boom"), () => {
          started.push("bad");
          throw new E.RuntimeError("boom");
        });
      } catch (e) {
        if (!(e instanceof E.RuntimeError)) throw e; // suppress noisy thread traceback
      }
    }
    function good() {
      with_(d.atomic("ok"), () => {
        AtomicDict.current()["x"] = 1;
      });
    }
    const tb = new TH.Thread({ target: bad });
    const tg = new TH.Thread({ target: good });
    tb.start();
    tb.join();
    tg.start();
    tg.join();
    for (const th of [tb, tg]) if (th.exception) throw th.exception;
    assert.ok(started.includes("bad"));
    assert.equal(d["x"], 1);
  });

  t("test_006_contextvar_isolation", () => {
    const d = new AtomicDict();
    const seen = [];
    function worker(i) {
      with_(d.atomic(`w${i}`), () => {
        seen.push(AtomicDict.current() instanceof AtomicDict);
      });
    }
    _spawn(worker);
    assert.ok(seen.every((x) => x));
  });

  t("test_007_run_atomic_multithread", () => {
    const d = new AtomicDict();
    function f() {
      const dd = AtomicDict.current();
      dd["x"] = dd.get("x", 0) + 1;
    }
    function worker() {
      d.run_atomic(f, "r");
    }
    _spawn(worker);
    assert.equal(d["x"], N_THREADS);
  });

  t("test_008_clear_vs_update", () => {
    const d = new AtomicDict();
    let stop = false;

    // Python: the *main* thread sleeps 0.2 s and then flips ``stop`` while the
    // updater spins. A cooperative main thread cannot run while a worker is
    // running, so the sleep+flip is a ``Timer`` thread, and the updater
    // yields (``sleep(0.001)``, as the clearer does) so the timer can fire.
    function updater() {
      let i = 0;
      while (!stop) {
        with_(d.atomic("u"), () => {
          AtomicDict.current()[String(i)] = i;
        });
        i += 1;
        time.sleep(0.001);
      }
    }
    function clearer() {
      for (let k = 0; k < 100; k++) {
        with_(d.atomic("c"), () => {
          d.clear();
        });
        time.sleep(0.001);
      }
    }
    const stopper = new TH.Timer(0.2, () => {
      stop = true;
    });
    const t1 = new TH.Thread({ target: updater });
    const t2 = new TH.Thread({ target: clearer });
    stopper.start();
    t1.start();
    t2.start();
    t1.join();
    t2.join();
    stopper.join();
    for (const th of [t1, t2]) if (th.exception) throw th.exception;
    assert.ok(Array.isArray(d.keys()));
  });

  t("test_009_trim_vs_update", () => {
    const d = new AtomicDict(Object.fromEntries(T.range(20).map((i) => [String(i), i])));
    function trimmer() {
      for (let k = 0; k < 50; k++) {
        with_(d.atomic("trim"), () => {
          d.trim(5, 15);
        });
      }
    }
    function updater() {
      for (let i = 0; i < 100; i++) {
        with_(d.atomic("upd"), () => {
          AtomicDict.current()[String(i % 20)] = i;
        });
      }
    }
    const t1 = new TH.Thread({ target: trimmer });
    const t2 = new TH.Thread({ target: updater });
    t1.start();
    t2.start();
    t1.join();
    t2.join();
    for (const th of [t1, t2]) if (th.exception) throw th.exception;
    assert.ok(d.keys().every((k) => /^\d+$/.test(k)));
  });

  t("test_010_deep_reentrancy", () => {
    const d = new AtomicDict();
    function work(level) {
      if (level === 0) {
        AtomicDict.current()["z"] = AtomicDict.current().get("z", 0) + 1;
      } else {
        with_(d.atomic(`L${level}`), () => {
          work(level - 1);
        });
      }
    }
    with_(d.atomic("L3"), () => {
      work(3);
    });
    assert.equal(d["z"], 1);
  });

  t("test_011_no_starvation_small", () => {
    const d = new AtomicDict();
    function worker(i) {
      for (let k = 0; k < 50; k++) {
        with_(d.atomic(`i${i}`), () => {
          d["n"] = d.get("n", 0) + 1;
        });
      }
    }
    _spawn(worker);
    assert.equal(d["n"], N_THREADS * 50);
  });

  t("test_012_mixed_patterns", () => {
    const d = new AtomicDict(Object.fromEntries(T.range(5).map((i) => [String(i), i])));
    function worker() {
      for (let k = 0; k < 50; k++) {
        with_(d.atomic("m"), () => {
          const dd = AtomicDict.current();
          if (dd.__len__()) dd.pop_next();
          dd[String(process.hrtime.bigint() % 5n)] = 1; // time.time_ns() % 5
        });
      }
    }
    _spawn(worker);
    assert.ok(T.len(d) >= 0);
  });

  t("test_013_unique_hints", () => {
    const d = new AtomicDict();
    function worker(i) {
      with_(d.atomic(`hint-${i}`), () => {
        d[String(i)] = i;
      });
    }
    _spawn(worker);
    assert.equal(T.len(d), N_THREADS);
  });

  t("test_014_current_outside_threads", () => {
    function worker() {
      assert.throws(() => AtomicDict.current(), E.RuntimeError);
    }
    _spawn(worker);
  });

  t("test_015_many_vs_one", () => {
    const d = new AtomicDict();
    function many() {
      for (let k = 0; k < 200; k++) {
        with_(d.atomic("s"), () => {
          d["x"] = d.get("x", 0) + 1;
        });
      }
    }
    function one() {
      with_(d.atomic("L"), () => {
        for (let k = 0; k < 200; k++) d["x"] = d.get("x", 0) + 1;
      });
    }
    const t1 = new TH.Thread({ target: many });
    const t2 = new TH.Thread({ target: one });
    t1.start();
    t2.start();
    t1.join();
    t2.join();
    for (const th of [t1, t2]) if (th.exception) throw th.exception;
    assert.equal(d["x"], 400);
  });

  t("test_016_cross_lock_orders", () => {
    const d1 = new AtomicDict();
    const d2 = new AtomicDict();
    function worker(i) {
      if (i % 2 === 0) {
        with_(d1.atomic("A"), () =>
          with_(d2.atomic("B"), () => {
            d1["a"] = d1.get("a", 0) + 1;
          }),
        );
      } else {
        with_(d2.atomic("B"), () =>
          with_(d1.atomic("A"), () => {
            d2["b"] = d2.get("b", 0) + 1;
          }),
        );
      }
    }
    _spawn(worker);
    assert.ok(d1.get("a", 0) + d2.get("b", 0) >= 1);
  });

  t("test_017_heavy_increment", () => {
    const d = new AtomicDict({ x: 0 });
    function worker() {
      for (let k = 0; k < 300; k++) {
        with_(d.atomic("h"), () => {
          d["x"] += 1;
        });
      }
    }
    _spawn(worker);
    assert.equal(d["x"], N_THREADS * 300);
  });

  t("test_018_clear_vs_pop_next", () => {
    const d = new AtomicDict(Object.fromEntries(T.range(10).map((i) => [String(i), i])));
    function popper() {
      for (let k = 0; k < 50; k++) {
        with_(d.atomic("p"), () => {
          if (T.len(d)) d.pop_next();
        });
      }
    }
    function clearer() {
      for (let k = 0; k < 10; k++) {
        with_(d.atomic("c"), () => {
          d.clear();
        });
        time.sleep(0.002);
      }
    }
    const t1 = new TH.Thread({ target: popper });
    const t2 = new TH.Thread({ target: clearer });
    t1.start();
    t2.start();
    t1.join();
    t2.join();
    for (const th of [t1, t2]) if (th.exception) throw th.exception;
    assert.ok(T.len(d) >= 0);
  });

  t("test_019_compute_contention", () => {
    const d = new AtomicDict();
    function worker() {
      for (let k = 0; k < 100; k++) {
        with_(d.atomic("comp"), () => {
          d.compute("z", (v) => (v === null ? 1 : v + 1));
        });
      }
    }
    _spawn(worker);
    assert.equal(d["z"], N_THREADS * 100);
  });

  t("test_020_inc_vs_compute", () => {
    const d = new AtomicDict();
    function inc() {
      for (let k = 0; k < 100; k++) {
        with_(d.atomic("i"), () => {
          d.increment("k");
        });
      }
    }
    function comp() {
      for (let k = 0; k < 100; k++) {
        with_(d.atomic("c"), () => {
          d.compute("k", (v) => (v === null ? 0 : v + 0));
        });
      }
    }
    const t1 = new TH.Thread({ target: inc });
    const t2 = new TH.Thread({ target: comp });
    t1.start();
    t2.start();
    t1.join();
    t2.join();
    for (const th of [t1, t2]) if (th.exception) throw th.exception;
    assert.equal(d["k"], 100);
  });

  t("test_021_values_alignment_stress", () => {
    const d = new AtomicDict();
    function worker(i) {
      for (let j = 0; j < 60; j++) {
        with_(d.atomic("a"), () => {
          d[String((i + j) % 10)] = i + j;
        });
      }
    }
    _spawn(worker);
    assert.equal(d.keys().length, d.values().length);
  });

  t("test_022_run_atomic_exception_threads", () => {
    const d = new AtomicDict();
    function f() {
      throw new E.ValueError("boom");
    }
    function worker() {
      assert.throws(() => d.run_atomic(f, "x"), E.ValueError);
    }
    _spawn(worker);
    d["ok"] = 1;
    assert.equal(d["ok"], 1);
  });

  t("test_023_flip_flop_key", () => {
    const d = new AtomicDict();
    function worker() {
      for (let k = 0; k < 100; k++) {
        with_(d.atomic("cd"), () => {
          if ("x" in d) delete d["x"];
          else d["x"] = 1;
        });
      }
    }
    _spawn(worker);
    assert.ok(Array.isArray(d.keys()));
  });

  t("test_024_trim_clear_vs_update", () => {
    const d = new AtomicDict(Object.fromEntries(T.range(8).map((i) => [String(i), i])));
    function trimmer() {
      for (let k = 0; k < 50; k++) {
        with_(d.atomic("t"), () => {
          d.trim(10, 5); // clears
        });
      }
    }
    function updater() {
      for (let i = 0; i < 100; i++) {
        with_(d.atomic("u"), () => {
          d[String(i % 4)] = i;
        });
      }
    }
    const t1 = new TH.Thread({ target: trimmer });
    const t2 = new TH.Thread({ target: updater });
    t1.start();
    t2.start();
    t1.join();
    t2.join();
    for (const th of [t1, t2]) if (th.exception) throw th.exception;
    assert.ok(T.len(d) >= 0);
  });

  t("test_025_iter_snapshot_concurrency", () => {
    const d = new AtomicDict(Object.fromEntries(T.range(20).map((i) => [String(i), i])));
    const snap = [...d]; // list(iter(d))
    function writer() {
      for (let i = 0; i < 100; i++) {
        with_(d.atomic("w"), () => {
          d[String(i % 20)] = i;
        });
      }
    }
    _spawn(writer, 2);
    assert.ok(snap.every((s) => typeof s === "string"));
  });

  t("test_026_pretty_vs_update", () => {
    const d = new AtomicDict(Object.fromEntries(T.range(5).map((i) => [String(i), i])));
    const outs = [];
    function pretty_worker() {
      for (let k = 0; k < 100; k++) outs.push(d.pretty());
    }
    function upd() {
      for (let i = 0; i < 100; i++) {
        with_(d.atomic("u"), () => {
          d[String(i % 5)] = i;
        });
      }
    }
    const t1 = new TH.Thread({ target: pretty_worker });
    const t2 = new TH.Thread({ target: upd });
    t1.start();
    t2.start();
    t1.join();
    t2.join();
    for (const th of [t1, t2]) if (th.exception) throw th.exception;
    assert.ok(outs.every((o) => typeof o === "string"));
  });

  t("test_027_compute_delete_vs_read", () => {
    const d = new AtomicDict({ x: 1 });
    function deleter() {
      for (let k = 0; k < 50; k++) {
        with_(d.atomic("del"), () => {
          d.compute("x", (_v) => null);
        });
      }
    }
    function reader() {
      for (let k = 0; k < 200; k++) d.get("x", 0);
    }
    const t1 = new TH.Thread({ target: deleter });
    const t2 = new TH.Thread({ target: reader });
    t1.start();
    t2.start();
    t1.join();
    t2.join();
    for (const th of [t1, t2]) if (th.exception) throw th.exception;
    assert.ok([0].includes(d.get("x", 0)));
  });

  t("test_028_views_consistency", () => {
    const d = new AtomicDict();
    function churn() {
      for (let i = 0; i < 200; i++) {
        with_(d.atomic("ch"), () => {
          d[String(i % 10)] = i;
          if (i % 3 === 0 && d.__len__()) d.pop_next();
        });
      }
    }
    _spawn(churn);
    assert.equal(d.keys().length, d.values().length);
    assert.equal(d.items().length, d.keys().length);
  });

  t("test_029_setdefault_contention", () => {
    const d = new AtomicDict();
    function worker(i) {
      for (let k = 0; k < 100; k++) {
        with_(d.atomic("sd"), () => {
          d.setdefault(String(i % 5), 0);
          d[String(i % 5)] += 1;
        });
      }
    }
    _spawn(worker);
    assert.equal(
      d.values().reduce((a, b) => a + b, 0),
      N_THREADS * 100,
    );
  });

  t("test_030_repr_vs_update", () => {
    const d = new AtomicDict({ a: 1 });
    const outs = [];
    function r() {
      for (let k = 0; k < 200; k++) outs.push(repr(d));
    }
    function u() {
      for (let i = 0; i < 200; i++) {
        with_(d.atomic("u"), () => {
          d["a"] = i;
        });
      }
    }
    const t1 = new TH.Thread({ target: r });
    const t2 = new TH.Thread({ target: u });
    t1.start();
    t2.start();
    t1.join();
    t2.join();
    for (const th of [t1, t2]) if (th.exception) throw th.exception;
    assert.ok(outs.some((s) => s.includes("AtomicDict")));
  });

  t("test_031_nested_run_atomic", () => {
    const d = new AtomicDict();
    function f() {
      d.run_atomic(() => AtomicDict.current().update(null, { x: 1 }), "inner");
    }
    d.run_atomic(f, "outer");
    assert.equal(d["x"], 1);
  });

  t("test_032_same_hint_ok", () => {
    const d = new AtomicDict();
    function worker() {
      for (let k = 0; k < 50; k++) {
        with_(d.atomic("same"), () => {
          d["x"] = d.get("x", 0) + 1;
        });
      }
    }
    _spawn(worker);
    assert.equal(d["x"], N_THREADS * 50);
  });

  t("test_033_long_vs_short_section", () => {
    const d = new AtomicDict();
    function long_() {
      with_(d.atomic("L"), () => {
        time.sleep(0.05);
        d["x"] = 1;
      });
    }
    function short_() {
      with_(d.atomic("S"), () => {
        d["y"] = d.get("y", 0) + 1;
      });
    }
    const t1 = new TH.Thread({ target: long_ });
    const t2 = new TH.Thread({ target: short_ });
    t1.start();
    t2.start();
    t1.join();
    t2.join();
    for (const th of [t1, t2]) if (th.exception) throw th.exception;
    assert.equal(d["x"], 1);
    assert.equal(d["y"], 1);
  });

  t("test_034_many_instances", () => {
    const dicts = T.range(10).map(() => new AtomicDict());
    function worker(i) {
      const d = dicts[i % dicts.length];
      with_(d.atomic("w"), () => {
        d["i"] = d.get("i", 0) + 1;
      });
    }
    _spawn(worker);
    assert.equal(
      dicts.reduce((acc, d) => acc + d.get("i", 0), 0),
      N_THREADS,
    );
  });

  t("test_035_concurrent_delete", () => {
    const d = new AtomicDict({ x: 1 });
    function worker() {
      with_(d.atomic("del"), () => {
        if ("x" in d) delete d["x"];
      });
    }
    _spawn(worker);
    assert.ok(!("x" in d));
  });

  t("test_036_popitem_vs_put", () => {
    const d = new AtomicDict(Object.fromEntries(T.range(5).map((i) => [String(i), i])));
    function popper() {
      for (let k = 0; k < 20; k++) {
        with_(d.atomic("p"), () => {
          if (T.len(d)) d.popitem();
        });
      }
    }
    function putter() {
      for (let i = 0; i < 20; i++) {
        with_(d.atomic("u"), () => {
          d[String(i)] = i;
        });
      }
    }
    const t1 = new TH.Thread({ target: popper });
    const t2 = new TH.Thread({ target: putter });
    t1.start();
    t2.start();
    t1.join();
    t2.join();
    for (const th of [t1, t2]) if (th.exception) throw th.exception;
    assert.ok(T.len(d) >= 0);
  });

  t("test_037_read_vs_write", () => {
    const d = new AtomicDict({ a: 1 });
    const reads = [];
    function reader() {
      for (let k = 0; k < 100; k++) reads.push(d.get("a", 0));
    }
    function writer() {
      for (let i = 0; i < 100; i++) {
        with_(d.atomic("w"), () => {
          d["a"] = i;
        });
      }
    }
    const t1 = new TH.Thread({ target: reader });
    const t2 = new TH.Thread({ target: writer });
    t1.start();
    t2.start();
    t1.join();
    t2.join();
    for (const th of [t1, t2]) if (th.exception) throw th.exception;
    assert.ok(reads.every((x) => T.is_int(x)));
  });

  t("test_038_moving_trim_window", () => {
    const d = new AtomicDict(Object.fromEntries(T.range(30).map((i) => [String(i), i])));
    function trimmer() {
      for (let j = 0; j < 60; j++) {
        with_(d.atomic("t"), () => {
          d.trim(j % 10, (j % 10) + 10);
        });
      }
    }
    function updater() {
      for (let i = 0; i < 200; i++) {
        with_(d.atomic("u"), () => {
          d[String(i % 30)] = i;
        });
      }
    }
    const t1 = new TH.Thread({ target: trimmer });
    const t2 = new TH.Thread({ target: updater });
    t1.start();
    t2.start();
    t1.join();
    t2.join();
    for (const th of [t1, t2]) if (th.exception) throw th.exception;
    assert.equal(d.keys().length, d.values().length);
  });

  t("test_039_remove_vs_increment", () => {
    const d = new AtomicDict({ x: 0 });
    function remover() {
      for (let k = 0; k < 50; k++) {
        with_(d.atomic("rm"), () => {
          d.compute("x", (_v) => null);
        });
      }
    }
    function increm() {
      for (let k = 0; k < 100; k++) {
        with_(d.atomic("in"), () => {
          d.increment("x");
        });
      }
    }
    const t1 = new TH.Thread({ target: remover });
    const t2 = new TH.Thread({ target: increm });
    t1.start();
    t2.start();
    t1.join();
    t2.join();
    for (const th of [t1, t2]) if (th.exception) throw th.exception;
    assert.ok(d.get("x", 0) >= 0);
  });

  t("test_040_pretty_and_repr_churn", () => {
    const d = new AtomicDict({ a: 1 });
    const outs = [];
    function churn() {
      for (let i = 0; i < 100; i++) {
        with_(d.atomic("u"), () => {
          d["a"] = i;
        });
        outs.push(d.pretty());
        outs.push(repr(d));
      }
    }
    _spawn(() => churn(), 2);
    assert.ok(outs.some((o) => o.includes("AtomicDict")));
  });

  t("test_041_contextvar_reset", () => {
    const d = new AtomicDict();
    with_(d.atomic("h"), () => {
      AtomicDict.current()["x"] = 1;
    });
    assert.throws(() => AtomicDict.current(), E.RuntimeError);
  });

  t("test_042_many_nested", () => {
    const d = new AtomicDict();
    function worker() {
      for (let k = 0; k < 50; k++) {
        with_(d.atomic("o"), () =>
          with_(d.atomic("i"), () => {
            d["v"] = d.get("v", 0) + 1;
          }),
        );
      }
    }
    _spawn(worker);
    assert.equal(d["v"], N_THREADS * 50);
  });

  t("test_043_run_atomic_returns", () => {
    const d = new AtomicDict({ x: 1 });
    const outs = [];
    function f() {
      return AtomicDict.current()["x"];
    }
    function worker() {
      outs.push(d.run_atomic(f, "r"));
    }
    _spawn(worker);
    assert.ok(outs.every((v) => v === 1));
  });

  t("test_044_order_after_recreate", () => {
    const d = new AtomicDict();
    function worker(i) {
      with_(d.atomic("k"), () => {
        const k = `k${i}`;
        d[k] = i;
        delete d[k];
        d[k] = i;
      });
    }
    _spawn(worker);
    assert.equal(T.len(d), N_THREADS);
  });

  t("test_045_long_chain", () => {
    const d = new AtomicDict();
    function worker() {
      for (let k = 0; k < 200; k++) {
        with_(d.atomic("a"), () => {
          d["x"] = d.get("x", 0) + 1;
        });
      }
    }
    _spawn(worker);
    assert.equal(d["x"], N_THREADS * 200);
  });

  t("test_046_barrier_inside_atomic", () => {
    // Barrier sync inside atomic -- ensure threads don't hang or corrupt state.
    const d = new AtomicDict();
    const barrier = new Barrier(N_THREADS);
    const errors = [];

    // Every party needs ``d``'s lock to reach the barrier, so (in Python as
    // here) only one thread is ever inside it: the wait times out, breaks the
    // barrier, and the remaining threads fail it immediately. See ``Barrier``
    // for why the waiter must not yield to the lock-blocked parties.
    function worker() {
      try {
        with_(d.atomic("b"), () => {
          try {
            barrier.wait(3, { yield_pending: false }); // safe timeout
          } catch (e) {
            // Allow barrier timeouts, but record them
            errors.push(e);
          }
          d["x"] = d.get("x", 0) + 1;
        });
      } catch (e) {
        errors.push(e);
      }
    }

    _spawn(worker);

    // The test passes as long as the dictionary isn't corrupted
    const n = d.get("x", 0);
    assert.ok(n > 0, "No threads updated the dict");
    assert.equal(d.keys().length, d.values().length, "Key/value mismatch");
    assert.ok(errors.length <= N_THREADS, `Too many errors: ${errors}`);
  });

  t("test_047_alternate_dicts", () => {
    const d1 = new AtomicDict();
    const d2 = new AtomicDict();
    function worker(i) {
      const d = i % 2 === 0 ? d1 : d2;
      with_(d.atomic("h"), () => {
        d["x"] = d.get("x", 0) + 1;
      });
    }
    _spawn(worker);
    assert.equal(d1.get("x", 0) + d2.get("x", 0), N_THREADS);
  });

  t("test_048_sliding_window_trim", () => {
    // Sliding-window trim under concurrent writes; assert bounded & consistent.
    const d = new AtomicDict(Object.fromEntries(T.range(50).map((i) => [String(i), i])));
    function trimmer() {
      for (let j = 0; j < 100; j++) {
        with_(d.atomic("t"), () => {
          d.trim(j % 40, (j % 40) + 10);
        });
      }
    }
    function writer() {
      for (let i = 0; i < 200; i++) {
        with_(d.atomic("w"), () => {
          d[String(i % 50)] = i;
        });
      }
    }
    const t1 = new TH.Thread({ target: trimmer });
    const t2 = new TH.Thread({ target: writer });
    t1.start();
    t2.start();
    t1.join();
    t2.join();
    for (const th of [t1, t2]) if (th.exception) throw th.exception;
    const n = T.len(d);
    assert.ok(n > 0, "Dictionary unexpectedly empty");
    assert.ok(n <= 50, `Dictionary grew unbounded (len=${n})`);
    assert.equal(d.keys().length, d.values().length);
  });

  t("test_049_mix_setdefault_increment", () => {
    const d = new AtomicDict();
    function worker(_i) {
      for (let k = 0; k < 100; k++) {
        with_(d.atomic("m"), () => {
          d.setdefault("k", 0);
          d.increment("k");
        });
      }
    }
    _spawn(worker);
    assert.equal(d["k"], N_THREADS * 100);
  });

  t("test_050_context_after_many", () => {
    const d = new AtomicDict();
    function worker() {
      for (let k = 0; k < 100; k++) {
        with_(d.atomic("z"), () => {
          AtomicDict.current()["z"] = AtomicDict.current().get("z", 0) + 1;
        });
      }
    }
    _spawn(worker);
    assert.equal(d["z"], N_THREADS * 100);
  });
});

// ── test_atomic_dict_functional.py ───────────────────────────────────────

describe("TestAtomicDictFunctional", () => {
  // Helpers available to all tests
  const _make_dict = (d = null) => new AtomicDict(d ?? {});
  const _range_dict = (n) => Object.fromEntries(T.range(n).map((i) => [String(i), i]));

  // 1. init with data= kw and positional dict
  test("test_001_init_variants", () => {
    const d1 = new AtomicDict(null, { data: { a: 1 } }); // AtomicDict(data={"a": 1})
    const d2 = new AtomicDict({ b: 2 });
    assert.equal(d1["a"], 1);
    assert.equal(d2["b"], 2);
  });

  // 2. conflicting init args
  test("test_002_conflicting_init", () => {
    assert.throws(() => new AtomicDict({ a: 1 }, { data: { b: 2 } }), E.TypeError);
  });

  // 3. order preserved after updates/mixed kwargs
  test("test_003_update_order", () => {
    const d = _make_dict({ a: 1 });
    d.update({ b: 2 });
    d.update([["c", 3]]);
    d.update(null, { d: 4, e: 5 });
    assert.deepEqual(d.keys(), ["a", "b", "c", "d", "e"]);
  });

  // 4. pop on missing with default
  test("test_004_pop_default", () => {
    const d = _make_dict();
    assert.equal(d.pop("x", 7), 7);
  });

  // 5. pop on missing without default raises
  test("test_005_pop_raises", () => {
    const d = _make_dict();
    assert.throws(() => d.pop("x"), E.KeyError);
  });

  // 6. popitem LIFO vs insertion order snapshot
  test("test_006_popitem_end", () => {
    const d = _make_dict({ a: 1, b: 2, c: 3 });
    const [k, v] = d.popitem();
    assert.deepEqual([k, v], ["c", 3]);
    assert.deepEqual(d.keys(), ["a", "b"]);
  });

  // 7. pop_next FIFO
  test("test_007_pop_next_start", () => {
    const d = _make_dict({ a: 1, b: 2, c: 3 });
    const [k, v] = d.pop_next();
    assert.deepEqual([k, v], ["a", 1]);
    assert.deepEqual(d.keys(), ["b", "c"]);
  });

  // 8. popitem on empty
  test("test_008_popitem_empty", () => {
    const d = _make_dict();
    assert.throws(() => d.popitem(), E.KeyError);
  });

  // 9. pop_next on empty
  test("test_009_pop_next_empty", () => {
    const d = _make_dict();
    assert.throws(() => d.pop_next(), E.KeyError);
  });

  // 10. setdefault existing/new
  test("test_010_setdefault", () => {
    const d = _make_dict({ a: 1 });
    assert.equal(d.setdefault("a", 9), 1);
    assert.equal(d.setdefault("b", 2), 2);
    assert.equal(d["b"], 2);
  });

  // 11. compute create/update/delete
  test("test_011_compute_lifecycle", () => {
    const d = _make_dict();
    d.compute("x", (v) => (v === null ? 1 : v + 1));
    d.compute("x", (v) => v + 4);
    assert.equal(d["x"], 5);
    d.compute("x", (_v) => null);
    assert.ok(!("x" in d));
  });

  // 12. increment default and negatives
  test("test_012_increment", () => {
    const d = _make_dict();
    d.increment("k");
    d.increment("k", 4);
    d.increment("k", -2);
    assert.equal(d["k"], 3);
  });

  // 13. __contains__
  test("test_013_contains", () => {
    const d = _make_dict({ a: 1 });
    assert.equal("a" in d, true);
    assert.equal("b" in d, false);
  });

  // 14. iteration snapshot not affected by later writes during iteration creation
  test("test_014_iter_snapshot", () => {
    const d = _make_dict({ a: 1, b: 2 });
    const it = d.__iter__();
    const list1 = [...it];
    d["c"] = 3;
    assert.deepEqual(list1, ["a", "b"]); // snapshot
    assert.deepEqual(d.keys(), ["a", "b", "c"]);
  });

  // 15. keys/values/items order
  test("test_015_views", () => {
    const d = _make_dict({ x: 1, y: 2 });
    assert.deepEqual(d.keys(), ["x", "y"]);
    assert.deepEqual(d.values(), [1, 2]);
    assert.deepEqual(pairs(d.items()), [["x", 1], ["y", 2]]);
  });

  // 16. item_at / key_at / value_at and negatives
  test("test_016_index_helpers", () => {
    const d = _make_dict({ a: 10, b: 20, c: 30 });
    assert.deepEqual([...d.item_at(0)], ["a", 10]);
    assert.equal(d.key_at(2), "c");
    assert.equal(d.value_at(-1), 30);
  });

  // 17. item_at out of range
  test("test_017_index_out_of_range", () => {
    const d = _make_dict({ a: 1 });
    assert.throws(() => d.item_at(2), E.IndexError);
  });

  // 18. trim inside bounds
  test("test_018_trim_window", () => {
    const d = _make_dict(_range_dict(5));
    d.trim(1, 4);
    assert.deepEqual(d.keys(), ["1", "2", "3"]);
  });

  // 19. trim with negatives and None
  test("test_019_trim_negative_end_none", () => {
    const d = _make_dict(_range_dict(5));
    d.trim(-3, null); // keep last 3
    assert.deepEqual(d.keys(), ["2", "3", "4"]);
  });

  // 20. trim clears when s>=e
  test("test_020_trim_clears", () => {
    const d = _make_dict({ a: 1, b: 2 });
    d.trim(10, 2);
    assert.equal(T.len(d), 0);
  });

  // 21. reindex mirrors data keys order
  test("test_021_reindex", () => {
    const d = _make_dict({ a: 1, b: 2 });
    d.reindex();
    assert.deepEqual(d.keys(), ["a", "b"]);
  });

  // 22. clear empties both data and order
  test("test_022_clear", () => {
    const d = _make_dict({ a: 1 });
    d.clear();
    assert.equal(T.len(d), 0);
    assert.deepEqual(d.keys(), []);
  });

  // 23. __repr__ contains ordered mapping
  test("test_023_repr_ordered", () => {
    const d = _make_dict({ a: 1, b: 2 });
    const r = repr(d);
    assert.ok(r.includes("AtomicDict"));
    assert.ok(r.indexOf("a") < r.indexOf("b"));
  });

  // 24. pretty includes braces and keys
  test("test_024_pretty_format", () => {
    const d = _make_dict({ a: 1, b: 2 });
    const s = d.pretty();
    assert.ok(s.trim().startsWith("AtomicDict {"));
    assert.ok(s.includes("'a': 1"));
  });

  // 25. update with iterable of pairs
  test("test_025_update_iterable", () => {
    const d = _make_dict();
    d.update([["a", 1], ["b", 2]]);
    assert.deepEqual(pairs(d.items()), [["a", 1], ["b", 2]]);
  });

  // 26. update mapping view
  test("test_026_update_mapping", () => {
    const d = _make_dict({ a: 1 });
    const m = { b: 2, c: 3 };
    d.update(m);
    assert.deepEqual(d.keys(), ["a", "b", "c"]);
  });

  // 27. __iter__ returns snapshot (explicit)
  test("test_027_iter_snapshot_again", () => {
    const d = _make_dict({ a: 1 });
    const keys_snapshot = [...d.__iter__()];
    d["b"] = 2;
    assert.deepEqual(keys_snapshot, ["a"]);
  });

  // 28. get default type preservation
  test("test_028_get_default_type", () => {
    const d = _make_dict();
    const got = d.get("x", T.tuple(["z"]));
    assert.ok(got instanceof T.PyTuple);
    assert.deepEqual(got, T.tuple(["z"]));
  });

  // 29. setitem preserves order when overwriting
  test("test_029_overwrite_keeps_order", () => {
    const d = _make_dict({ a: 1, b: 2 });
    d["a"] = 9;
    assert.deepEqual(d.keys(), ["a", "b"]);
    assert.equal(d["a"], 9);
  });

  // 30. compute should keep insertion order if updating existing
  test("test_030_compute_order", () => {
    const d = _make_dict({ a: 1, b: 2 });
    d.compute("a", (v) => v + 1);
    assert.deepEqual(d.keys(), ["a", "b"]);
  });

  // 31. compute creates at end for new key
  test("test_031_compute_insert_end", () => {
    const d = _make_dict({ a: 1 });
    d.compute("z", (v) => (v === null ? 10 : v));
    assert.deepEqual(d.keys(), ["a", "z"]);
  });

  // 32. increment creates at end for new key
  test("test_032_increment_insert_end", () => {
    const d = _make_dict({ a: 1 });
    d.increment("z", 3);
    assert.deepEqual(d.keys(), ["a", "z"]);
  });

  // 33. __len__ after mutations
  test("test_033_len", () => {
    const d = _make_dict();
    d["a"] = 1;
    d["b"] = 2;
    delete d["a"];
    d["c"] = 3;
    assert.equal(T.len(d), 2);
  });

  // 34. __contains__ false for missing even after trim
  test("test_034_contains_after_trim", () => {
    const d = _make_dict({ a: 1, b: 2 });
    d.trim(1, 2);
    assert.equal("a" in d, false);
  });

  // 35. items() reflects values after mutation
  test("test_035_items_reflect_mutation", () => {
    const d = _make_dict({ a: 1 });
    d["a"] = 9;
    assert.deepEqual(pairs(d.items()), [["a", 9]]);
  });

  // 36. get within atomic context via current()
  test("test_036_current_in_atomic", () => {
    const d = _make_dict({ a: 1 });
    with_(d.atomic("h"), () => {
      const dd = AtomicDict.current();
      assert.equal(dd.get("a"), 1);
    });
  });

  // 37. current() outside context raises
  test("test_037_current_outside", () => {
    assert.throws(() => AtomicDict.current(), E.RuntimeError);
  });

  // 38. atomic() hint must be str
  test("test_038_hint_type", () => {
    const d = _make_dict();
    assert.throws(() => d.atomic(123), E.TypeError);
  });

  // 39. run_atomic returns function result
  test("test_039_run_atomic_return", () => {
    const d = _make_dict({ x: 1 });
    function f() {
      const dd = AtomicDict.current();
      return dd["x"] + 1;
    }
    assert.equal(d.run_atomic(f, "r"), 2);
  });

  // 40. run_atomic propagates exceptions and releases lock
  test("test_040_run_atomic_exception", () => {
    const d = _make_dict({ x: 1 });
    function boom() {
      throw new E.ValueError("x");
    }
    assert.throws(() => d.run_atomic(boom, "x"), E.ValueError);
    // Should still be usable
    d["y"] = 2;
    assert.equal(d["y"], 2);
  });

  // 41. _AtomicView supports update/assign/delete under lock
  test("test_041_atomicview_ops", () => {
    const d = _make_dict();
    with_(d.atomic("v"), () => {
      const v = new AtomicDict._AtomicView(d);
      v.update(null, { a: 1 });
      v["b"] = 2;
      delete v["a"];
    });
    assert.deepEqual(pairs(d.items()), [["b", 2]]);
  });

  // 42. keys reflects insertion after multiple compute
  test("test_042_compute_multiple", () => {
    const d = _make_dict();
    for (const k of ["a", "b", "c"]) d.compute(k, (_v) => 1);
    assert.deepEqual(d.keys(), ["a", "b", "c"]);
  });

  // 43. update order when overwriting existing keys
  test("test_043_update_overwrite_order", () => {
    const d = _make_dict({ a: 1, b: 2 });
    d.update({ a: 9 });
    assert.deepEqual(d.keys(), ["a", "b"]);
    assert.equal(d["a"], 9);
  });

  // 44. trim no-op full range
  test("test_044_trim_full", () => {
    const d = _make_dict(_range_dict(3));
    d.trim(0, 3);
    assert.deepEqual(d.keys(), ["0", "1", "2"]);
  });

  // 45. negative start clamped
  test("test_045_trim_negative_start_clamp", () => {
    const d = _make_dict(_range_dict(3));
    d.trim(-10, 2);
    assert.deepEqual(d.keys(), ["0", "1"]);
  });

  // 46. end beyond length clamped
  test("test_046_trim_end_clamp", () => {
    const d = _make_dict(_range_dict(3));
    d.trim(1, 99);
    assert.deepEqual(d.keys(), ["1", "2"]);
  });

  // 47. remove then re-add appears at end
  test("test_047_remove_readd_order", () => {
    const d = _make_dict({ a: 1, b: 2 });
    delete d["a"];
    d["a"] = 9;
    assert.deepEqual(d.keys(), ["b", "a"]);
  });

  // 48. len matches number of keys after many ops
  test("test_048_len_after_many_ops", () => {
    const d = _make_dict();
    for (let i = 0; i < 10; i++) d[String(i)] = i;
    for (let i = 0; i < 10; i += 2) delete d[String(i)];
    assert.equal(T.len(d), 5);
  });

  // 49. __contains__ with non-existing object type
  test("test_049_contains_object_type", () => {
    const d = _make_dict({ a: 1 });
    assert.equal(d.__contains__(123), false);
  });

  // 50. keys/values remain aligned after mutations
  test("test_050_keys_values_alignment", () => {
    const d = _make_dict({ a: 1, b: 2 });
    d["b"] = 20;
    d["a"] = 10;
    assert.deepEqual(
      [...T.zip(d.keys(), d.values())].map((p) => [...p]),
      [["a", 10], ["b", 20]],
    );
  });
});

// ── test_atomic_dotmap.py ────────────────────────────────────────────────

describe("TestAtomicDotMap", () => {
  test("test_set_get_attr", () => {
    const dm = new AtomicDotMap();
    dm.s3_bucket_region = "us-east-1";
    assert.equal(dm.s3_bucket_region, "us-east-1");
  });

  test("test_delete_attr", () => {
    const dm = new AtomicDotMap();
    dm.token = "abc";
    delete dm.token;
    assert.equal(dm.token, null);
  });

  test("test_to_dict_keys_items", () => {
    const dm = new AtomicDotMap();
    dm.a = 1;
    dm.b = 2;
    assert.deepEqual(dm.to_dict(), { a: 1, b: 2 });
    assert.deepEqual(T.sorted(dm.keys()), ["a", "b"]);
    assert.deepEqual(T.sorted(pairs(dm.items())), [["a", 1], ["b", 2]]);
  });

  test("test_repr", () => {
    const dm = new AtomicDotMap();
    dm.x = 3;
    assert.ok(repr(dm).includes("AtomicDotMap"));
  });

  t("test_concurrent_sets", () => {
    const dm = new AtomicDotMap();
    const n_threads = 10;

    function worker(i) {
      dm.__setattr__(`k${i}`, i);
    }

    const threads = T.range(n_threads).map((i) => new TH.Thread({ target: worker, args: [i] }));
    for (const th of threads) th.start();
    for (const th of threads) th.join();
    for (const th of threads) if (th.exception) throw th.exception;

    const data = dm.to_dict();
    assert.equal(Object.keys(data).length, n_threads);
    for (let i = 0; i < n_threads; i++) assert.equal(data[`k${i}`], i);
  });
});

// ── test_atomic_flag.py ──────────────────────────────────────────────────

describe("TestAtomicFlag", () => {
  test("test_default_false", () => {
    const f = new AtomicFlag();
    assert.equal(f.is_set(), false);
  });

  test("test_set_clear_toggle", () => {
    const f = new AtomicFlag();
    f.set();
    assert.equal(f.is_set(), true);
    f.toggle();
    assert.equal(f.is_set(), false);
    f.clear();
    assert.equal(f.is_set(), false);
  });

  test("test_set_to", () => {
    const f = new AtomicFlag();
    f.set_to(true);
    assert.equal(f.is_set(), true);
    f.set_to(false);
    assert.equal(f.is_set(), false);
  });

  test("test_atomic_context_direct_mutation", () => {
    const f = new AtomicFlag();
    with_(f.atomic(), (locked) => {
      locked.value = true;
    });
    assert.equal(f.is_set(), true);
  });

  t("test_concurrent_toggle_is_safe", () => {
    const f = new AtomicFlag();
    const n_threads = 10;
    const n_steps = 1000;

    function worker() {
      for (let k = 0; k < n_steps; k++) f.toggle();
    }

    const threads = T.range(n_threads).map(() => new TH.Thread({ target: worker }));
    for (const th of threads) th.start();
    for (const th of threads) th.join();
    for (const th of threads) if (th.exception) throw th.exception;

    assert.ok([true, false].includes(f.is_set()));
  });
});

// ── test_atomic_int.py ───────────────────────────────────────────────────

describe("TestAtomicInt", () => {
  test("test_default_zero", () => {
    const i = new AtomicInt();
    assert.equal(i.get(), 0);
  });

  test("test_set_add_increment_decrement", () => {
    const i = new AtomicInt();
    i.set_to(5);
    assert.equal(i.get(), 5);
    assert.equal(i.add(3), 8);
    assert.equal(i.increment(), 9);
    assert.equal(i.decrement(), 8);
  });

  test("test_reset", () => {
    const i = new AtomicInt();
    i.set_to(99);
    i.reset();
    assert.equal(i.get(), 0);
  });

  test("test_atomic_context", () => {
    const i = new AtomicInt();
    with_(i.atomic(), (locked) => {
      locked.value += 10;
    });
    assert.equal(i.get(), 10);
  });

  t("test_concurrent_increments", () => {
    const i = new AtomicInt();
    const n_threads = 8;
    const n_steps = 500;

    function worker() {
      for (let k = 0; k < n_steps; k++) i.increment();
    }

    const threads = T.range(n_threads).map(() => new TH.Thread({ target: worker }));
    for (const th of threads) th.start();
    for (const th of threads) th.join();
    for (const th of threads) if (th.exception) throw th.exception;

    assert.equal(i.get(), n_threads * n_steps);
  });
});

// ── test_atomic_list.py ──────────────────────────────────────────────────

describe("TestAtomicList", () => {
  test("test_default_empty", () => {
    const lst = new AtomicList();
    assert.equal(T.len(lst), 0);
    assert.deepEqual(lst.to_list(), []);
  });

  test("test_append_extend_insert_pop", () => {
    const lst = new AtomicList();
    lst.append(1);
    lst.extend([2, 3]);
    lst.insert(1, 9);
    assert.deepEqual(lst.to_list(), [1, 9, 2, 3]);
    assert.equal(lst.pop(), 3);
    assert.deepEqual(lst.to_list(), [1, 9, 2]);
  });

  test("test_set_get_slice", () => {
    const lst = new AtomicList({ value: [0, 1, 2, 3, 4] });
    lst.set_at(2, 99);
    assert.equal(lst.get_at(2), 99);
    assert.deepEqual(lst.slice(1, 4), [1, 99, 3]);
  });

  test("test_trim", () => {
    const lst = new AtomicList({ value: [0, 1, 2, 3, 4] });
    lst.trim(1, 4);
    assert.deepEqual(lst.to_list(), [1, 2, 3]);
  });

  test("test_atomic_context_batch", () => {
    const lst = new AtomicList();
    with_(lst.atomic(), (raw) => {
      raw.push(...[1, 2, 3]); // ``raw.extend([1, 2, 3])`` on the underlying list
    });
    assert.deepEqual(lst.to_list(), [1, 2, 3]);
  });

  t("test_concurrent_appends", () => {
    const lst = new AtomicList();
    const n_threads = 6;
    const n_steps = 200;

    function worker(idx) {
      for (let k = 0; k < n_steps; k++) lst.append(idx);
    }

    const threads = T.range(n_threads).map((i) => new TH.Thread({ target: worker, args: [i] }));
    for (const th of threads) th.start();
    for (const th of threads) th.join();
    for (const th of threads) if (th.exception) throw th.exception;

    assert.equal(T.len(lst), n_threads * n_steps);
  });
});

// ── test_atomic_str.py ───────────────────────────────────────────────────

describe("TestAtomicStr", () => {
  test("test_default_empty", () => {
    const s = new AtomicStr();
    assert.equal(s.get(), "");
    assert.equal(s.length(), 0);
  });

  test("test_set_append_clear", () => {
    const s = new AtomicStr();
    s.set("ab");
    assert.equal(s.get(), "ab");
    assert.equal(s.append("cd"), "abcd");
    assert.equal(s.length(), 4);
    s.clear();
    assert.equal(s.get(), "");
  });

  test("test_atomic_context", () => {
    const s = new AtomicStr();
    with_(s.atomic(), (locked) => {
      locked.value = "locked";
    });
    assert.equal(s.get(), "locked");
  });

  test("test_str_dunder", () => {
    const s = new AtomicStr();
    s.set("hello");
    assert.equal(T.str(s), "hello");
  });

  t("test_concurrent_appends", () => {
    const s = new AtomicStr();
    const n_threads = 5;
    const n_steps = 100;

    function worker(ch) {
      for (let k = 0; k < n_steps; k++) s.append(ch);
    }

    const threads = T.range(n_threads).map((i) => new TH.Thread({ target: worker, args: [String.fromCharCode(65 + i)] }));
    for (const th of threads) th.start();
    for (const th of threads) th.join();
    for (const th of threads) if (th.exception) throw th.exception;

    assert.equal(s.length(), n_threads * n_steps);
  });
});
