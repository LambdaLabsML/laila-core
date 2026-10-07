/**
 * Port of ``tests/deep_eval/test_01_atomics_deep.py``.
 *
 * Deep unit tests for the thread-safe atomic primitives.
 *
 * Covers ``AtomicInt``, ``AtomicFlag``, ``AtomicStr``, ``AtomicList``,
 * ``AtomicDict``, ``AtomicDotMap`` and the ``_LAILA_LOCALLY_ATOMIC_OBJECT``
 * lock protocol.
 */
import { test, describe } from "node:test";
import assert from "node:assert/strict";

import { S, macrotask } from "./_fixtures.js";

const T = await import(S + "_compat/pytypes.js");
const E = await import(S + "_compat/errors.js");
const TH = await import(S + "_compat/threading.js");
const time = await import(S + "_compat/time.js");
const { ThreadPoolExecutor } = await import(S + "_compat/executor.js");
const { with_ } = await import(S + "_compat/contextlib.js");
const { deepcopy } = await import(S + "_compat/copy.js");
const { repr } = await import(S + "_compat/pyrepr.js");
const { _LAILA_LOCALLY_ATOMIC_OBJECT } = await import(S + "atomic/definitions/locally_atomic_object.js");
const { AtomicDict } = await import(S + "atomic/types/atomic_dict.js");
const { AtomicDotMap } = await import(S + "atomic/types/atomic_dotmap.js");
const { AtomicFlag } = await import(S + "atomic/types/atomic_flag.js");
const { AtomicInt } = await import(S + "atomic/types/atomic_int.js");
const { AtomicList } = await import(S + "atomic/types/atomic_list.js");
const { AtomicStr } = await import(S + "atomic/types/atomic_str.js");

const { range, tuple } = T;
const t = (name, fn, opts) => (opts ? test(name, opts, () => macrotask(fn)) : test(name, () => macrotask(fn)));

/** ``with ThreadPoolExecutor(n) as ex: list(ex.map(fn, iterable))`` */
function pool_map(n, fn, iterable) {
  return with_(new ThreadPoolExecutor({ max_workers: n }), (ex) => ex.map(fn, iterable));
}

/**
 * ``Event.wait()`` while the current context holds a lock.
 *
 * The threading shim refuses a blocking wait while a lock is held (rule L1,
 * deadlock guard) because the completing callback could never run underneath
 * the holder; the Python tests do exactly that on purpose, so wait through
 * the raw pump with the guard disabled -- a unified lock would still time
 * out, exactly as in CPython.
 */
function wait_holding_lock(event, timeout) {
  return TH.blocking_wait(() => event.is_set(), timeout, { allow_locked: true });
}

// ---------------------------------------------------------------------------
// AtomicInt
// ---------------------------------------------------------------------------

describe("TestAtomicInt", () => {
  t("test_default_zero", () => {
    assert.equal(new AtomicInt().get(), 0);
  });

  for (const start of [-5, 0, 1, 2 ** 40]) {
    t(`test_initial_value[${start}]`, () => {
      assert.equal(new AtomicInt({ value: start }).get(), start);
    });
  }

  t("test_add_returns_new_value", () => {
    const a = new AtomicInt({ value: 10 });
    assert.equal(a.add(5), 15);
    assert.equal(a.get(), 15);
  });

  t("test_add_negative", () => {
    const a = new AtomicInt({ value: 10 });
    assert.equal(a.add(-20), -10);
  });

  t("test_increment_decrement", () => {
    const a = new AtomicInt();
    assert.equal(a.increment(), 1);
    assert.equal(a.increment(), 2);
    assert.equal(a.decrement(), 1);
  });

  t("test_set_to_coerces_to_int", () => {
    const a = new AtomicInt();
    a.set_to(3.9);
    assert.equal(a.get(), 3);
    assert.ok(T.is_int(a.get()));
  });

  t("test_set_to_bool", () => {
    const a = new AtomicInt();
    a.set_to(true);
    assert.equal(a.get(), 1);
  });

  t("test_reset", () => {
    const a = new AtomicInt({ value: 99 });
    a.reset();
    assert.equal(a.get(), 0);
  });

  t("test_int_conversion", () => {
    const a = new AtomicInt({ value: 7 });
    assert.equal(a.__int__(), 7);
    assert.equal(+a, 7);
    assert.equal(a.get() + 1, 8);
  });

  t("test_atomic_context_returns_parent", () => {
    const a = new AtomicInt({ value: 1 });
    with_(a.atomic(), (inner) => {
      assert.equal(inner, a);
      inner.add(1);
    });
    assert.equal(a.get(), 2);
  });

  t("test_atomic_context_is_reentrant", () => {
    const a = new AtomicInt();
    with_(a.atomic(), () => {
      with_(a.atomic(), () => {
        a.increment();
      });
    });
    assert.equal(a.get(), 1);
  });

  t("test_concurrent_increments_are_exact", () => {
    const a = new AtomicInt();
    const [n_threads, per_thread] = [16, 2000];

    const worker = () => {
      for (let i = 0; i < per_thread; i++) a.increment();
    };

    pool_map(n_threads, () => worker(), range(n_threads));
    assert.equal(a.get(), n_threads * per_thread);
  });

  t("test_concurrent_mixed_add", () => {
    const a = new AtomicInt();

    const worker = (delta) => {
      for (let i = 0; i < 1000; i++) a.add(delta);
    };

    pool_map(8, worker, [1, -1, 2, -2, 3, -3, 4, -4]);
    assert.equal(a.get(), 0);
  });

  t("test_has_creation_timestamp", () => {
    const a = new AtomicInt();
    assert.equal(typeof a.creation_timestamp, "string");
    assert.ok(a.creation_timestamp.includes("T"));
  });

  t("test_internal_lock_differs_from_base_local_lock", () => {
    // Methods use ``_lock`` while the inherited ``lock()`` uses ``_local_lock``.
    //
    // Documented as a potential inconsistency: holding ``a.lock()`` does
    // *not* exclude ``a.increment()`` from another thread.
    const a = new AtomicInt();
    assert.ok(a.lock(1));
    try {
      const done = new TH.Event();

      const other = () => {
        a.increment();
        done.set();
      };

      new TH.Thread({ target: other }).start();
      // If the two locks were the same object this would time out.
      assert.ok(wait_holding_lock(done, 1.0), "increment blocked by lock() -- locks are unified");
    } finally {
      a.unlock();
    }
    assert.equal(a.get(), 1);
  });

  t("test_lock_timeout_returns_false_when_contended", () => {
    const a = new AtomicInt();
    const acquired = new TH.Event();
    const release = new TH.Event();

    // The holder waits *while holding the lock*; in the single-threaded
    // shim a blocked holder must yield asynchronously so the main context
    // can run its contended ``lock(timeout_s=...)`` underneath.
    const holder = async () => {
      a.lock();
      acquired.set();
      await release.wait_async();
      a.unlock();
    };

    const th = new TH.Thread({ target: holder });
    th.start();
    acquired.wait();
    assert.equal(a.lock(0.05), false);
    release.set();
    th.join();
    if (th.exception) throw th.exception;
  });

  t("test_locked_property", () => {
    const a = new AtomicInt();
    assert.equal(a.locked(), false);
    a.lock();
    assert.equal(a.locked(), true);
    a.unlock();
    assert.equal(a.locked(), false);
  });

  t("test_atomic_scope_rejects_unknown", () => {
    const a = new AtomicInt();
    assert.throws(() => {
      // ``"scope" in a.atomic.__code__.co_varnames`` -> does ``atomic`` take a parameter?
      if (a.atomic.length > 0) with_(a.atomic({ scope: "global" }), () => {});
      else _raise();
    });
  });
});

function _raise() {
  throw new E.ValueError("scope not accepted");
}

// ---------------------------------------------------------------------------
// AtomicFlag
// ---------------------------------------------------------------------------

describe("TestAtomicFlag", () => {
  t("test_default_false", () => {
    assert.equal(new AtomicFlag().is_set(), false);
  });

  t("test_set_clear", () => {
    const f = new AtomicFlag();
    f.set();
    assert.ok(f.is_set());
    f.clear();
    assert.ok(!f.is_set());
  });

  t("test_toggle", () => {
    const f = new AtomicFlag();
    f.toggle();
    assert.ok(f.is_set());
    f.toggle();
    assert.ok(!f.is_set());
  });

  for (const state of [true, false, 1, 0, "x", ""]) {
    t(`test_set_to_truthiness[${repr(state)}]`, () => {
      const f = new AtomicFlag();
      f.set_to(state);
      assert.equal(f.is_set(), T.bool(state));
    });
  }

  t("test_bool_protocol", () => {
    const f = new AtomicFlag();
    assert.ok(!T.bool(f));
    f.set();
    assert.ok(T.bool(f));
  });

  t("test_atomic_context", () => {
    const f = new AtomicFlag();
    with_(f.atomic(), (inner) => {
      inner.set();
    });
    assert.ok(f.is_set());
  });

  t("test_concurrent_toggle_even_count", () => {
    const f = new AtomicFlag();

    const worker = () => {
      for (let i = 0; i < 1000; i++) f.toggle();
    };

    pool_map(8, () => worker(), range(8));
    assert.equal(f.is_set(), false);
  });
});

// ---------------------------------------------------------------------------
// AtomicStr
// ---------------------------------------------------------------------------

describe("TestAtomicStr", () => {
  t("test_default_empty", () => {
    assert.equal(new AtomicStr().get(), "");
  });

  t("test_set_get", () => {
    const s = new AtomicStr();
    s.set("abc");
    assert.equal(s.get(), "abc");
  });

  t("test_append_returns_new", () => {
    const s = new AtomicStr({ value: "a" });
    assert.equal(s.append("b"), "ab");
    assert.equal(s.get(), "ab");
  });

  t("test_clear", () => {
    const s = new AtomicStr({ value: "abc" });
    s.clear();
    assert.equal(s.get(), "");
    assert.equal(s.length(), 0);
  });

  t("test_length_unicode", () => {
    const s = new AtomicStr({ value: "héllo" });
    assert.equal(s.length(), 5);
  });

  t("test_str_protocol", () => {
    const s = new AtomicStr({ value: "x" });
    assert.equal(T.str(s), "x");
  });

  t("test_no_len_protocol_but_length_method", () => {
    const s = new AtomicStr({ value: "xyz" });
    assert.equal(s.length(), 3);
    assert.throws(() => T.len(s), E.TypeError); // not part of the contract; documents current behaviour
  });

  t("test_concurrent_append_length", () => {
    const s = new AtomicStr();

    const worker = () => {
      for (let i = 0; i < 500; i++) s.append("ab");
    };

    pool_map(8, () => worker(), range(8));
    assert.equal(s.length(), 8 * 500 * 2);
  });

  t("test_atomic_context", () => {
    const s = new AtomicStr();
    with_(s.atomic(), (inner) => {
      inner.append("q");
    });
    assert.equal(s.get(), "q");
  });
});

// ---------------------------------------------------------------------------
// AtomicList
// ---------------------------------------------------------------------------

describe("TestAtomicList", () => {
  t("test_default_empty", () => {
    assert.equal(T.len(new AtomicList()), 0);
    assert.deepEqual(new AtomicList().to_list(), []);
  });

  t("test_initial_values", () => {
    const lst = new AtomicList({ value: [1, 2, 3] });
    assert.deepEqual(lst.to_list(), [1, 2, 3]);
  });

  t("test_append_extend", () => {
    const lst = new AtomicList();
    lst.append(1);
    lst.extend([2, 3]);
    assert.deepEqual(lst.to_list(), [1, 2, 3]);
  });

  t("test_insert", () => {
    const lst = new AtomicList({ value: [1, 3] });
    lst.insert(1, 2);
    assert.deepEqual(lst.to_list(), [1, 2, 3]);
  });

  t("test_insert_out_of_range_appends", () => {
    const lst = new AtomicList({ value: [1] });
    lst.insert(100, 2);
    assert.deepEqual(lst.to_list(), [1, 2]);
  });

  t("test_remove_missing_raises", () => {
    const lst = new AtomicList({ value: [1] });
    assert.throws(() => lst.remove(99), E.ValueError);
  });

  t("test_pop_default_last", () => {
    const lst = new AtomicList({ value: [1, 2, 3] });
    assert.equal(lst.pop(), 3);
    assert.equal(lst.pop(0), 1);
    assert.deepEqual(lst.to_list(), [2]);
  });

  t("test_pop_empty_raises", () => {
    assert.throws(() => new AtomicList().pop(), E.IndexError);
  });

  t("test_count_index", () => {
    const lst = new AtomicList({ value: [1, 2, 2, 3] });
    assert.equal(lst.count(2), 2);
    assert.equal(lst.index(2), 1);
    assert.equal(lst.index(2, 2), 2);
  });

  t("test_index_missing_raises", () => {
    assert.throws(() => new AtomicList({ value: [1] }).index(5), E.ValueError);
  });

  t("test_to_list_is_snapshot", () => {
    const lst = new AtomicList({ value: [1] });
    const snap = lst.to_list();
    lst.append(2);
    assert.deepEqual(snap, [1]);
  });

  t("test_set_at_get_at", () => {
    const lst = new AtomicList({ value: [1, 2] });
    lst.set_at(0, 9);
    assert.equal(lst.get_at(0), 9);
    assert.equal(lst.get_at(-1), 2);
  });

  t("test_get_at_out_of_range", () => {
    assert.throws(() => new AtomicList({ value: [1] }).get_at(5), E.IndexError);
  });

  t("test_slice_method", () => {
    const lst = new AtomicList({ value: range(10) });
    assert.deepEqual(lst.slice(2, 6), [2, 3, 4, 5]);
    assert.deepEqual(lst.slice(null, null, 3), [0, 3, 6, 9]);
  });

  t("test_trim", () => {
    const lst = new AtomicList({ value: range(10) });
    lst.trim(2, 5);
    assert.deepEqual(lst.to_list(), [2, 3, 4]);
  });

  t("test_trim_negative_bounds", () => {
    const lst = new AtomicList({ value: range(10) });
    lst.trim(-3, null);
    assert.deepEqual(lst.to_list(), [7, 8, 9]);
  });

  t("test_getitem_int_and_slice", () => {
    const lst = new AtomicList({ value: [1, 2, 3] });
    assert.equal(lst[0], 1);
    assert.equal(lst[-1], 3);
    assert.deepEqual(lst.__getitem__({ start: 0, stop: 2 }), [1, 2]);
  });

  t("test_setitem_delitem", () => {
    const lst = new AtomicList({ value: [1, 2, 3] });
    lst[1] = 20;
    delete lst[0];
    assert.deepEqual(lst.to_list(), [20, 3]);
  });

  t("test_iter_snapshot_safe_during_mutation", () => {
    const lst = new AtomicList({ value: [1, 2, 3] });
    const seen = [];
    for (const x of lst) {
      seen.push(x);
      lst.append(x * 10);
    }
    assert.deepEqual(seen, [1, 2, 3]);
    assert.equal(T.len(lst), 6);
  });

  t("test_clear", () => {
    const lst = new AtomicList({ value: [1, 2] });
    lst.clear();
    assert.equal(T.len(lst), 0);
  });

  t("test_repr", () => {
    assert.equal(repr(new AtomicList({ value: [1] })), "AtomicList([1])");
  });

  t("test_concurrent_append_count", () => {
    const lst = new AtomicList();

    const worker = (i) => {
      for (let j = 0; j < 500; j++) lst.append(tuple([i, j]));
    };

    pool_map(8, worker, range(8));
    assert.equal(T.len(lst), 4000);
    // ``len(set(...))``: tuples hash by value
    assert.equal(new Set(lst.to_list().map((p) => p.join(","))).size, 4000);
  });

  t("test_atomic_context_batches", () => {
    const lst = new AtomicList();
    with_(lst.atomic(), (inner) => {
      // ``inner`` is the raw underlying list
      inner.push(1);
      inner.push(2);
    });
    assert.deepEqual(lst.to_list(), [1, 2]);
  });
});

// ---------------------------------------------------------------------------
// AtomicDict
// ---------------------------------------------------------------------------

describe("TestAtomicDictConstruction", () => {
  t("test_empty", () => {
    const d = new AtomicDict();
    assert.equal(T.len(d), 0);
    assert.deepEqual(d.keys(), []);
  });

  t("test_positional_dict", () => {
    const d = new AtomicDict({ a: 1, b: 2 });
    assert.deepEqual(d.keys(), ["a", "b"]);
    assert.equal(d["a"], 1);
  });

  t("test_data_kwarg", () => {
    const d = new AtomicDict(null, { data: { x: 1 } });
    assert.equal(d["x"], 1);
  });

  t("test_both_positional_and_data_raises", () => {
    assert.throws(() => new AtomicDict({ a: 1 }, { data: { b: 2 } }), E.TypeError);
  });

  t("test_two_positionals_raises", () => {
    assert.throws(() => new AtomicDict({ a: 1 }, { b: 2 }, {}), E.TypeError);
  });

  t("test_non_dict_positional_raises", () => {
    assert.throws(() => new AtomicDict([["a", 1]]), E.TypeError);
  });

  t("test_unexpected_kwarg_raises", () => {
    assert.throws(() => new AtomicDict(null, { foo: 1 }), E.TypeError);
  });

  t("test_initial_order_matches_dict", () => {
    const src = { z: 1, y: 2, x: 3 };
    const d = new AtomicDict(src);
    assert.deepEqual(d.keys(), ["z", "y", "x"]);
  });

  t("test_positional_dict_is_not_copied", () => {
    // Documented behaviour check: the positional dict is used *by reference*.
    const src = { a: 1 };
    const d = new AtomicDict(src);
    src["b"] = 2;
    // Either behaviour is acceptable, but it must be consistent with ``data``
    assert.equal(d.data === src, T.dict_has(d.data, "b"));
  });
});

describe("TestAtomicDictMapping", () => {
  t("test_setitem_getitem", () => {
    const d = new AtomicDict();
    d["a"] = 1;
    assert.equal(d["a"], 1);
  });

  t("test_getitem_missing_raises", () => {
    assert.throws(() => new AtomicDict().__getitem__("nope"), E.KeyError);
  });

  t("test_delitem", () => {
    const d = new AtomicDict({ a: 1 });
    delete d["a"];
    assert.ok(!("a" in d));
    assert.deepEqual(d.keys(), []);
  });

  t("test_delitem_missing_raises", () => {
    assert.throws(() => new AtomicDict().__delitem__("x"), E.KeyError);
  });

  t("test_contains", () => {
    const d = new AtomicDict({ a: 1 });
    assert.ok("a" in d);
    assert.ok(!("b" in d));
  });

  t("test_len", () => {
    const d = new AtomicDict({ a: 1, b: 2 });
    assert.equal(T.len(d), 2);
  });

  t("test_iter_follows_insertion_order", () => {
    const d = new AtomicDict();
    for (const k of "cab") d[k] = 1;
    assert.deepEqual([...d], ["c", "a", "b"]);
  });

  t("test_reinsert_existing_key_keeps_position", () => {
    const d = new AtomicDict({ a: 1, b: 2 });
    d["a"] = 10;
    assert.deepEqual(d.keys(), ["a", "b"]);
    assert.equal(d["a"], 10);
  });

  t("test_get_default", () => {
    const d = new AtomicDict();
    assert.equal(d.get("x"), null);
    assert.equal(d.get("x", 5), 5);
  });

  t("test_setdefault_existing", () => {
    const d = new AtomicDict({ a: 1 });
    assert.equal(d.setdefault("a", 9), 1);
  });

  t("test_setdefault_missing", () => {
    const d = new AtomicDict();
    assert.equal(d.setdefault("a", 9), 9);
    assert.equal(d["a"], 9);
  });

  t("test_pop_existing", () => {
    const d = new AtomicDict({ a: 1 });
    assert.equal(d.pop("a"), 1);
    assert.equal(T.len(d), 0);
  });

  t("test_pop_missing_default", () => {
    assert.equal(new AtomicDict().pop("a", 7), 7);
  });

  t("test_pop_missing_no_default_raises", () => {
    assert.throws(() => new AtomicDict().pop("a"), E.KeyError);
  });

  t("test_pop_none_default_is_distinct_from_missing", () => {
    const d = new AtomicDict();
    assert.equal(d.pop("a", null), null);
  });

  t("test_popitem_lifo", () => {
    const d = new AtomicDict({ a: 1, b: 2 });
    assert.deepEqual([...d.popitem()], ["b", 2]);
    assert.deepEqual([...d.popitem()], ["a", 1]);
  });

  t("test_popitem_empty_raises", () => {
    assert.throws(() => new AtomicDict().popitem(), E.KeyError);
  });

  t("test_pop_next_fifo", () => {
    const d = new AtomicDict({ a: 1, b: 2 });
    assert.deepEqual([...d.pop_next()], ["a", 1]);
    assert.deepEqual([...d.pop_next()], ["b", 2]);
  });

  t("test_pop_next_empty_raises", () => {
    assert.throws(() => new AtomicDict().pop_next(), E.KeyError);
  });

  t("test_pop_next_after_order_desync", () => {
    const d = new AtomicDict({ a: 1, b: 2 });
    T.dict_set(d.data, "c", 3); // out-of-band
    assert.deepEqual([...d.pop_next()], ["a", 1]);
    assert.deepEqual(d.keys(), ["b", "c"]);
  });

  t("test_clear", () => {
    const d = new AtomicDict({ a: 1 });
    d.clear();
    assert.equal(T.len(d), 0);
    assert.deepEqual(d.keys(), []);
  });

  t("test_update_mapping", () => {
    const d = new AtomicDict();
    d.update({ a: 1, b: 2 });
    assert.deepEqual(
      d.items().map((kv) => [...kv]),
      [
        ["a", 1],
        ["b", 2],
      ],
    );
  });

  t("test_update_pairs", () => {
    const d = new AtomicDict();
    d.update([tuple(["a", 1]), tuple(["b", 2])]);
    assert.deepEqual(d.keys(), ["a", "b"]);
  });

  t("test_update_kwargs", () => {
    const d = new AtomicDict();
    d.update(null, { x: 1, y: 2 });
    assert.deepEqual(d.keys(), ["x", "y"]);
  });

  t("test_update_none", () => {
    const d = new AtomicDict({ a: 1 });
    d.update(null);
    assert.deepEqual(d.keys(), ["a"]);
  });

  t("test_values_items", () => {
    const d = new AtomicDict({ a: 1, b: 2 });
    assert.deepEqual(d.values(), [1, 2]);
    assert.deepEqual(
      d.items().map((kv) => [...kv]),
      [
        ["a", 1],
        ["b", 2],
      ],
    );
  });

  t("test_item_at", () => {
    const d = new AtomicDict({ a: 1, b: 2 });
    assert.deepEqual([...d.item_at(0)], ["a", 1]);
    assert.deepEqual([...d.item_at(-1)], ["b", 2]);
  });

  t("test_item_at_out_of_range", () => {
    const d = new AtomicDict({ a: 1 });
    assert.throws(() => d.item_at(1), E.IndexError);
    assert.throws(() => d.item_at(-2), E.IndexError);
  });

  t("test_key_at_value_at", () => {
    const d = new AtomicDict({ a: 1, b: 2 });
    assert.equal(d.key_at(1), "b");
    assert.equal(d.value_at(1), 2);
  });

  const abcdef = () => Object.fromEntries([..."abcdef"].map((k, i) => [k, i]));

  t("test_trim_middle", () => {
    const d = new AtomicDict(abcdef());
    d.trim(1, 4);
    assert.deepEqual(d.keys(), ["b", "c", "d"]);
  });

  t("test_trim_negative", () => {
    const d = new AtomicDict(abcdef());
    d.trim(-2);
    assert.deepEqual(d.keys(), ["e", "f"]);
  });

  t("test_trim_empty_range_clears", () => {
    const d = new AtomicDict({ a: 1, b: 2 });
    d.trim(2, 1);
    assert.equal(T.len(d), 0);
  });

  t("test_trim_default_noop", () => {
    const d = new AtomicDict({ a: 1, b: 2 });
    d.trim();
    assert.deepEqual(d.keys(), ["a", "b"]);
  });

  t("test_compute_set", () => {
    const d = new AtomicDict();
    assert.equal(
      d.compute("a", (cur) => (cur ?? 0) + 5),
      5,
    );
    assert.equal(d["a"], 5);
  });

  t("test_compute_remove", () => {
    const d = new AtomicDict({ a: 1 });
    assert.equal(
      d.compute("a", (_cur) => null),
      null,
    );
    assert.ok(!("a" in d));
  });

  t("test_compute_remove_missing_noop", () => {
    const d = new AtomicDict();
    assert.equal(
      d.compute("a", (_cur) => null),
      null,
    );
    assert.equal(T.len(d), 0);
  });

  t("test_increment_default", () => {
    const d = new AtomicDict();
    assert.equal(d.increment("a"), 1);
    assert.equal(d.increment("a", 5), 6);
  });

  t("test_increment_custom_default", () => {
    const d = new AtomicDict();
    assert.equal(d.increment("a", 1, 10), 11);
  });

  t(
    "test_increment_non_numeric_type_error",
    () => {
      const d = new AtomicDict({ a: "x" });
      assert.throws(() => d.increment("a"), E.TypeError);
    },
  );

  t("test_pretty", () => {
    const d = new AtomicDict({ a: 1 });
    const out = d.pretty();
    assert.ok(out.startsWith("AtomicDict {"));
    assert.ok(out.includes("'a': 1,"));
  });

  t("test_reindex_after_out_of_band_write", () => {
    const d = new AtomicDict({ a: 1 });
    T.dict_set(d.data, "b", 2);
    d.reindex();
    assert.deepEqual(d.keys(), ["a", "b"]);
  });

  t("test_length_based_sync_detects_added_key", () => {
    const d = new AtomicDict({ a: 1 });
    T.dict_set(d.data, "b", 2);
    assert.deepEqual(d.keys(), ["a", "b"]);
  });

  t("test_same_length_out_of_band_replacement_is_detected", () => {
    const d = new AtomicDict({ a: 1 });
    T.dict_del(d.data, "a");
    T.dict_set(d.data, "b", 2);
    assert.deepEqual(
      d.items().map((kv) => [...kv]),
      [["b", 2]],
    );
  });

  t("test_has_creation_timestamp", () => {
    assert.equal(typeof new AtomicDict().creation_timestamp, "string");
  });

  t("test_equality_with_other_mappings", () => {
    const d = new AtomicDict({ a: 1 });
    // MutableMapping provides __eq__ via Mapping mixin
    assert.deepEqual(Object.fromEntries(d.items()), { a: 1 });
  });

  t("test_nested_values_not_copied", () => {
    const inner = [1];
    const d = new AtomicDict({ a: inner });
    d["a"].push(2);
    assert.deepEqual(inner, [1, 2]);
  });
});

describe("TestAtomicDictAtomicBlock", () => {
  t("test_view_setitem_delitem", () => {
    const d = new AtomicDict({ a: 1 });
    with_(d.atomic(), (view) => {
      view["b"] = 2;
      delete view["a"];
    });
    assert.deepEqual(d.keys(), ["b"]);
  });

  t("test_view_update", () => {
    const d = new AtomicDict();
    with_(d.atomic(), (view) => {
      view.update({ a: 1 }, { b: 2 });
      view.update([tuple(["c", 3])]);
    });
    assert.deepEqual(d.keys(), ["a", "b", "c"]);
  });

  t("test_current_inside_block", () => {
    const d = new AtomicDict();
    with_(d.atomic(), () => {
      assert.equal(AtomicDict.current(), d);
    });
  });

  t("test_current_outside_block_raises", () => {
    assert.throws(() => AtomicDict.current(), E.RuntimeError);
  });

  t("test_current_reset_after_block", () => {
    const d = new AtomicDict();
    with_(d.atomic(), () => {});
    assert.throws(() => AtomicDict.current(), E.RuntimeError);
  });

  t("test_current_reset_after_exception", () => {
    const d = new AtomicDict();
    assert.throws(
      () =>
        with_(d.atomic(), () => {
          throw new E.ValueError();
        }),
      E.ValueError,
    );
    assert.throws(() => AtomicDict.current(), E.RuntimeError);
    // lock released: another thread can acquire
    assert.equal(
      d.run_atomic(() => 1),
      1,
    );
  });

  t("test_nested_atomic_blocks", () => {
    const [d1, d2] = [new AtomicDict(), new AtomicDict()];
    with_(d1.atomic(), () => {
      with_(d2.atomic(), () => {
        assert.equal(AtomicDict.current(), d2);
      });
      assert.equal(AtomicDict.current(), d1);
    });
  });

  t("test_hint_type_checked", () => {
    const d = new AtomicDict();
    assert.throws(() => d.atomic(5), E.TypeError);
    assert.throws(() => d.run_atomic(() => 1, 5), E.TypeError);
  });

  t("test_hint_stored", () => {
    const d = new AtomicDict();
    const ctx = d.atomic("xyz");
    assert.equal(ctx.hint, "xyz");
  });

  t("test_run_atomic_returns", () => {
    const d = new AtomicDict({ a: 1 });
    assert.equal(
      d.run_atomic(() => d["a"] + 1),
      2,
    );
  });

  t("test_run_atomic_propagates_exception", () => {
    const d = new AtomicDict();

    const boom = () => {
      throw new E.KeyError("k");
    };

    assert.throws(() => d.run_atomic(boom), E.KeyError);
  });

  t("test_atomic_block_excludes_other_threads", () => {
    const d = new AtomicDict();
    const order = [];
    const entered = new TH.Event();
    const release = new TH.Event();

    // The holder parks inside its atomic block (asynchronously, see the
    // ``lock_timeout`` test) so the waiter can block on the dict lock
    // underneath it.
    const holder = async () => {
      const cm = d.atomic();
      cm.__enter__();
      try {
        entered.set();
        await release.wait_async();
        order.push("holder");
      } finally {
        cm.__exit__(null, null, null);
      }
    };

    const waiter = () => {
      entered.wait();
      d["x"] = 1;
      order.push("waiter");
    };

    const t1 = new TH.Thread({ target: holder });
    const t2 = new TH.Thread({ target: waiter });
    t1.start();
    t2.start();
    // ``entered.wait(); time.sleep(0.05); assert order == []; release.set()``
    // -- the main context cannot be resumed while the waiter blocks on the
    // lock above it in the stack, so the 50 ms check + release run on a
    // timer and the snapshot is asserted after the joins.
    let snapshot = null;
    const timer = setTimeout(() => {
      snapshot = [...order];
      release.set();
    }, 50);
    entered.wait();
    t1.join();
    t2.join();
    clearTimeout(timer);
    if (t1.exception) throw t1.exception;
    if (t2.exception) throw t2.exception;
    assert.deepEqual(snapshot, []);
    assert.deepEqual(order, ["holder", "waiter"]);
  });

  t("test_concurrent_increments", () => {
    const d = new AtomicDict();

    const worker = () => {
      for (let i = 0; i < 1000; i++) d.increment("k");
    };

    pool_map(8, () => worker(), range(8));
    assert.equal(d["k"], 8000);
  });

  t("test_concurrent_producer_consumer_pop_next", () => {
    const d = new AtomicDict();
    const consumed = [];
    const stop = new TH.Event();

    const producer = () => {
      for (let i = 0; i < 2000; i++) d.__setitem__(i, i);
    };

    const consumer = () => {
      while (!stop.is_set() || T.len(d)) {
        try {
          consumed.push(d.pop_next()[1]);
        } catch (e) {
          if (!(e instanceof E.KeyError)) throw e;
          time.sleep(0.0005);
        }
      }
    };

    const c = new TH.Thread({ target: consumer });
    c.start();
    producer();
    stop.set();
    c.join();
    if (c.exception) throw c.exception;
    assert.deepEqual(
      [...consumed].sort((x, y) => x - y),
      range(2000),
    );
  });

  t("test_concurrent_distinct_keys", () => {
    const d = new AtomicDict();

    const worker = (i) => {
      for (let j = 0; j < 300; j++) d.__setitem__(tuple([i, j]), j);
    };

    pool_map(8, worker, range(8));
    assert.equal(T.len(d), 2400);
    assert.equal(d.keys().length, 2400);
  });
});

// ---------------------------------------------------------------------------
// AtomicDotMap
// ---------------------------------------------------------------------------

describe("TestAtomicDotMap", () => {
  t("test_missing_attr_is_none", () => {
    const m = new AtomicDotMap();
    assert.equal(m.anything, null);
  });

  t("test_set_get", () => {
    const m = new AtomicDotMap();
    m.a = 1;
    assert.equal(m.a, 1);
  });

  t("test_delattr", () => {
    const m = new AtomicDotMap();
    m.a = 1;
    delete m.a;
    assert.equal(m.a, null);
  });

  t("test_delattr_missing_raises", () => {
    const m = new AtomicDotMap();
    assert.throws(() => {
      delete m.nothing;
    }, E.KeyError);
  });

  t("test_to_dict", () => {
    const m = new AtomicDotMap();
    m.a = 1;
    m.b = 2;
    assert.deepEqual(m.to_dict(), { a: 1, b: 2 });
  });

  t("test_to_dict_is_copy", () => {
    const m = new AtomicDotMap();
    m.a = 1;
    const d = m.to_dict();
    d["a"] = 2;
    assert.equal(m.a, 1);
  });

  t("test_keys_values_items", () => {
    const m = new AtomicDotMap();
    m.a = 1;
    assert.deepEqual([...m.keys()], ["a"]);
    assert.deepEqual([...m.values()], [1]);
    assert.deepEqual(
      [...m.items()].map((kv) => [...kv]),
      [["a", 1]],
    );
  });

  t("test_internal_attrs_not_exposed", () => {
    const m = new AtomicDotMap();
    assert.ok(!("_data" in m.to_dict()));
    assert.ok(!("_lock" in m.to_dict()));
  });

  t("test_creation_timestamp", () => {
    const m = new AtomicDotMap();
    assert.equal(typeof m.creation_timestamp, "string");
  });

  t("test_nested_values", () => {
    const m = new AtomicDotMap();
    m.cfg = { x: 1 };
    assert.equal(m.cfg["x"], 1);
  });

  t("test_concurrent_writes", () => {
    const m = new AtomicDotMap();

    const worker = (i) => {
      for (let j = 0; j < 200; j++) m[`k${i}_${j}`] = j;
    };

    pool_map(8, worker, range(8));
    assert.equal(Object.keys(m.to_dict()).length, 1600);
  });

  t("test_lock_api_from_base", () => {
    const m = new AtomicDotMap();
    assert.ok(m.lock(1));
    m.unlock();
  });
});

// ---------------------------------------------------------------------------
// _LAILA_LOCALLY_ATOMIC_OBJECT protocol
// ---------------------------------------------------------------------------

class _Obj extends _LAILA_LOCALLY_ATOMIC_OBJECT {}

describe("TestLocallyAtomicObject", () => {
  t("test_lock_unlock", () => {
    const o = new _Obj();
    assert.equal(o.lock(), true);
    assert.ok(o.locked());
    o.unlock();
    assert.ok(!o.locked());
  });

  t("test_unlock_without_lock_raises", () => {
    const o = new _Obj();
    assert.throws(() => o.unlock(), E.RuntimeError);
  });

  t("test_atomic_local_scope", () => {
    const o = new _Obj();
    with_(o.atomic(), () => {
      assert.ok(o.locked());
    });
    assert.ok(!o.locked());
  });

  t("test_atomic_unknown_scope_raises", () => {
    const o = new _Obj();
    assert.throws(() => with_(o.atomic({ scope: "galactic" }), () => {}));
  });

  t("test_atomic_timeout_when_held", () => {
    const o = new _Obj();
    const held = new TH.Event();
    const release = new TH.Event();

    const holder = async () => {
      const cm = o.atomic();
      cm.__enter__();
      try {
        held.set();
        await release.wait_async();
      } finally {
        cm.__exit__(null, null, null);
      }
    };

    const th = new TH.Thread({ target: holder });
    th.start();
    held.wait();
    try {
      assert.throws(() => with_(o.atomic({ timeout_s: 0.05 }), () => {}));
    } finally {
      release.set();
      th.join();
    }
    if (th.exception) throw th.exception;
  });

  t("test_reentrant", () => {
    const o = new _Obj();
    with_(o.atomic(), () => {
      with_(o.atomic(), () => {
        assert.ok(o.locked());
      });
    });
  });

  t("test_lock_is_per_instance", () => {
    const [a, b] = [new _Obj(), new _Obj()];
    a.lock();
    assert.ok(!b.locked());
    a.unlock();
  });

  t("test_deepcopy_atomic_dict", () => {
    const d = new AtomicDict({ a: [1] });
    const c = deepcopy(d);
    c["a"].push(2);
    assert.deepEqual(d["a"], [1]);
    assert.deepEqual(c.keys(), ["a"]);
  });
});
