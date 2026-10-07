/**
 * Central memory sub-package: ports of
 *   tests/functional/policy/memory/record/unit_tests/test_memory_record.py
 *   tests/functional/policy/memory/base/unit_tests/test_base_memory.py
 *   tests/functional/policy/memory/alpha_pool/unit_tests/test_alpha_pool_routing.py
 *   tests/functional/policy/memory/manifest/unit_tests/test_manifest.py
 *   tests/functional/policy/memory/manifest/unit_tests/test_manifest_sql.py
 *   tests/functional/policy/memory/manifest/unit_tests/test_manifest_direct_resolver.py
 *
 * ``test_manifest_heavy_workloads.py`` (starved 1x1 taskforces driving
 * ``laila.build`` recursion) is ported with the rest of the functional tree
 * in p9.
 */
import { test, describe, beforeEach, afterEach } from "node:test";
import assert from "node:assert/strict";
import crypto from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import v8 from "node:v8";
import vm from "node:vm";

const { default: laila, S } = await import("./fixtures/laila_root.js");

const E = await import(S + "_compat/errors.js");
const T = await import(S + "_compat/pytypes.js");
const asyncio = await import(S + "_compat/asyncio.js");
const time = await import(S + "_compat/time.js");
const { with_ } = await import(S + "_compat/contextlib.js");
const { repr } = await import(S + "_compat/pyrepr.js");
const { deepcopy } = await import(S + "_compat/copy.js");
const defaults = await import(S + "macros/defaults.js");
const { LAILA_DEFAULT_DIRECTORIES } = defaults;
const { _DEFAULT_POOL_NICKNAME } = await import(S + "macros/strings.js");
const { _LAILA_IDENTIFIABLE_POOL } = await import(S + "data/schema/base.js");
const { PoolWrapper } = await import(S + "data/schema/pool_wrapper.js");
const { _LAILA_IDENTIFIABLE_POLICY } = await import(S + "policy/schema/base.js");
const CMD = await import(S + "policy/central/command/index.js");
const { GroupFuture, _LAILA_IDENTIFIABLE_FUTURE, CyclicDependencyError, _RESOLVE_CHAIN } = CMD;
const { _LAILA_IDENTIFIABLE_CENTRAL_MEMORY } = await import(S + "policy/central/memory/schema/base.js");
const { _LAILA_IDENTIFIABLE_POOL_ROUTER } = await import(S + "policy/central/memory/router/pool_router.js");
const { Manifest } = await import(S + "policy/central/memory/schema/manifest.js");
const { Record } = await import(S + "policy/central/memory/record/record.js");
const { Entry, EntryState } = await import(S + "entry/index.js");

const { NotImplemented } = T;
const str = (x) => (x && typeof x.__str__ === "function" ? x.__str__() : String(x));
const _T = 30.0;

const TMP_ROOT = fs.mkdtempSync(path.join(os.tmpdir(), "laila_memory_test_"));
laila.set_default_directory(TMP_ROOT);

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
const t = (name, fn) => test(name, () => macrotask(fn));
const sorted = (xs) => [...xs].sort();
const count_equal = (a, b) => assert.deepEqual(sorted(a), sorted(b));
const _unwrap = (r) => (r !== null && typeof r === "object" && "data" in r ? r.data : r);
const _make_gid = (scope = "ENTRY") => `LAILA:${scope}:${crypto.randomUUID()}`;
const _bank = () => laila.get_active_policy().future_bank;

/** ``unittest.mock.Mock(return_value=...)`` -- records every call. */
function Mock(return_value = undefined) {
  const fn = (...args) => {
    fn.calls.push(args);
    return return_value;
  };
  fn.calls = [];
  fn.assert_called_once = () => assert.equal(fn.calls.length, 1, `expected 1 call, got ${fn.calls.length}`);
  fn.assert_not_called = () => assert.equal(fn.calls.length, 0, `expected no calls, got ${fn.calls.length}`);
  fn.assert_called_once_with = (...expected) => {
    fn.assert_called_once();
    assert.deepEqual(fn.calls[0], expected);
  };
  Object.defineProperty(fn, "call_args", { get: () => ({ args: fn.calls[fn.calls.length - 1] }) });
  return fn;
}

/** ``setUp`` / ``tearDown`` of the policy-backed suites. */
function use_test_policy(ctx, { fresh_memory = false, register_local = false } = {}) {
  beforeEach(() =>
    macrotask(() => {
      ctx.original = laila.get_active_policy();
      ctx.policy = new _LAILA_IDENTIFIABLE_POLICY();
      laila.activate_policy(ctx.policy);
      if (register_local) laila._local_policies[ctx.policy.global_id] = ctx.policy;
      if (fresh_memory) {
        ctx.memory = new _LAILA_IDENTIFIABLE_CENTRAL_MEMORY();
        ctx.policy.central.memory = ctx.memory;
      } else {
        ctx.memory = ctx.policy.central.memory;
      }
    }),
  );
  afterEach(() =>
    macrotask(() => {
      try {
        laila.get_active_policy().central.command.shutdown({ wait: true, cancel_pending: true });
      } catch {
        // ignore
      }
      if (register_local) delete laila._local_policies[ctx.policy.global_id];
      laila.activate_policy(ctx.original);
    }),
  );
}

// ---------------------------------------------------------------------------
// test_memory_record.py
// ---------------------------------------------------------------------------
describe("TestRecord", () => {
  t("1 genesis entry id from global id", () => {
    const e = laila.constant([1, 2, 3, 4]);
    const r = new Record({ entry: e, recorder: "c", borrower: "b" });
    assert.equal(r.entry_id, e.global_id);
  });
  t("2 archetype entry id from global id", () => {
    const e = laila.variable([1, 2, 3, 4]);
    const r = new Record({ entry: e, recorder: "c", borrower: "b" });
    assert.equal(r.entry_id, e.global_id);
  });
  t("3 dict entry id from _global_id key", () => {
    const payload = { _global_id: "LAILA:ENTRY:XYZ", data: [1, 2, 3, 4] };
    const r = new Record({ entry: payload, recorder: "c", borrower: "b" });
    assert.equal(r.entry_id, "LAILA:ENTRY:XYZ");
  });
  t("4 dict missing _global_id raises key error", () => {
    const r = new Record({ entry: { data: [1, 2, 3] }, recorder: "c", borrower: "b" });
    assert.throws(() => r.entry_id, E.KeyError);
  });
  t("5 invalid entry type raises", () => {
    const r = new Record({ entry: new (class Opaque {})(), recorder: "c", borrower: "b" });
    assert.throws(() => r.entry_id, E.TypeError);
  });
  t("6 record timestamp default factory returns string", () => {
    const r = new Record({ entry: { _global_id: "LAILA:ENTRY:T1" }, recorder: "c", borrower: "b" });
    assert.equal(typeof r.record_timestamp, "string");
    assert.ok(r.record_timestamp.length > 0);
  });
  t("7 record timestamp can be provided", () => {
    const ts = "2020-01-02T03:04:05";
    const r = new Record({ entry: { _global_id: "LAILA:ENTRY:T2" }, recorder: "c", borrower: "b", record_timestamp: ts });
    assert.equal(r.record_timestamp, ts);
  });
  t("8 model dump does not include entry_id property", () => {
    const r = new Record({ entry: { _global_id: "LAILA:ENTRY:T4" }, recorder: "c", borrower: "b" });
    const d = r.model_dump();
    for (const k of ["entry", "recorder", "borrower", "record_timestamp"]) assert.ok(k in d, k);
    assert.ok(!("entry_id" in d));
  });
  t("9 as_dict includes entry payload", () => {
    const r = new Record({ entry: { _global_id: "LAILA:ENTRY:T5" }, recorder: "c", borrower: "b" });
    const d = r.as_dict;
    assert.ok("entry" in d);
    assert.deepEqual(d.entry, { _global_id: "LAILA:ENTRY:T5" });
  });
});

// ---------------------------------------------------------------------------
// test_base_memory.py :: TestBaseMemory
// ---------------------------------------------------------------------------
describe("TestBaseMemory", () => {
  const ctx = {};
  use_test_policy(ctx, { fresh_memory: true });

  t("model_post_init sets pool router", () => {
    assert.notEqual(ctx.memory.pool_router, null);
    assert.ok(ctx.memory.pool_router instanceof _LAILA_IDENTIFIABLE_POOL_ROUTER);
  });

  t("extend adds into actual router", () => {
    const pool = new _LAILA_IDENTIFIABLE_POOL();
    ctx.memory.extend(pool, { affinity: 0.25, pool_nickname: "secondary" });
    assert.ok(pool.global_id in ctx.memory.pool_router.pools);
    assert.equal(ctx.memory.pool_router.pools_nicknames["secondary"], pool.global_id);
  });

  t("record dispatches to non batch", () => {
    const pool = new _LAILA_IDENTIFIABLE_POOL({ batch_accelerated: false });
    ctx.memory._parallel_individual_record = Mock("non-batch");
    ctx.memory._batch_accelerated_record = Mock("batch");
    assert.equal(ctx.memory._record(["e"], pool), "non-batch");
    ctx.memory._parallel_individual_record.assert_called_once();
    ctx.memory._batch_accelerated_record.assert_not_called();
  });

  t("record dispatches to batch", () => {
    const pool = new _LAILA_IDENTIFIABLE_POOL({ batch_accelerated: true });
    ctx.memory._parallel_individual_record = Mock("non-batch");
    ctx.memory._batch_accelerated_record = Mock("batch");
    assert.equal(ctx.memory._record(["e"], pool), "batch");
    ctx.memory._batch_accelerated_record.assert_called_once();
    ctx.memory._parallel_individual_record.assert_not_called();
  });

  t("fetch borrow true raises not implemented", () => {
    assert.throws(() => ctx.memory._fetch(["id1"], { pool: new _LAILA_IDENTIFIABLE_POOL(), borrow: true }), E.NotImplementedError);
  });

  t("fetch dispatches by batch flag", () => {
    ctx.memory._parallel_individual_fetch = Mock("non-batch-fetch");
    ctx.memory._batch_accelerated_fetch = Mock("batch-fetch");
    const out_non_batch = ctx.memory._fetch(["id1"], { pool: new _LAILA_IDENTIFIABLE_POOL({ batch_accelerated: false }) });
    const out_batch = ctx.memory._fetch(["id1"], { pool: new _LAILA_IDENTIFIABLE_POOL({ batch_accelerated: true }) });
    assert.equal(out_non_batch, "non-batch-fetch");
    assert.equal(out_batch, "batch-fetch");
  });

  t("delete dispatches by batch flag", () => {
    ctx.memory._parallel_individual_delete = Mock("non-batch-delete");
    ctx.memory._batch_accelerated_delete = Mock("batch-delete");
    const out_non_batch = ctx.memory._delete(["id1"], new _LAILA_IDENTIFIABLE_POOL({ batch_accelerated: false }));
    const out_batch = ctx.memory._delete(["id1"], new _LAILA_IDENTIFIABLE_POOL({ batch_accelerated: true }));
    assert.equal(out_non_batch, "non-batch-delete");
    assert.equal(out_batch, "batch-delete");
  });

  t("memorize returns futures for default pool", () => {
    ctx.memory._record = Mock("futures");
    assert.equal(ctx.memory.memorize([laila.constant(1)]), "futures");
    ctx.memory._record.assert_called_once();
  });

  t("memorize returns futures for non default pool", () => {
    const routed = new _LAILA_IDENTIFIABLE_POOL();
    ctx.memory.extend(routed, { pool_nickname: "secondary" });
    ctx.memory._record = Mock("futures");
    assert.equal(ctx.memory.memorize([laila.constant(1)], { pool_id: routed.global_id }), "futures");
  });

  t("memorize single entry is wrapped to list", () => {
    const routed = new _LAILA_IDENTIFIABLE_POOL();
    ctx.memory.extend(routed, { pool_nickname: "secondary" });
    ctx.memory._record = Mock("futures");
    const e = laila.constant(1);
    ctx.memory.memorize(e, { pool_id: routed.global_id });
    const recorded = ctx.memory._record.call_args.args[0];
    assert.ok(Array.isArray(recorded));
    assert.equal(recorded.length, 1);
    assert.equal(recorded[0], e);
  });

  t("remember returns futures for default pool", () => {
    ctx.memory._fetch = Mock("fetched");
    assert.equal(ctx.memory.remember(["id1"]), "fetched");
    ctx.memory._fetch.assert_called_once();
  });

  t("remember returns futures for non default pool", () => {
    const routed = new _LAILA_IDENTIFIABLE_POOL();
    ctx.memory.extend(routed, { pool_nickname: "secondary" });
    ctx.memory._fetch = Mock("fetched");
    assert.equal(ctx.memory.remember(["id1"], { pool_id: routed.global_id, persist: false }), "fetched");
    ctx.memory._fetch.assert_called_once();
  });

  t("forget returns futures for default pool", () => {
    ctx.memory._delete = Mock("deleted");
    assert.equal(ctx.memory.forget(["id1"]), "deleted");
    ctx.memory._delete.assert_called_once();
  });

  t("forget returns futures for non default pool", () => {
    const routed = new _LAILA_IDENTIFIABLE_POOL();
    ctx.memory.extend(routed, { pool_nickname: "secondary" });
    ctx.memory._delete = Mock("deleted");
    assert.equal(ctx.memory.forget(["id1"], { pool_id: routed.global_id }), "deleted");
    ctx.memory._delete.assert_called_once();
  });

  t("router has default pool nickname mapping", () => {
    assert.ok(_DEFAULT_POOL_NICKNAME in ctx.memory.pool_router.pools_nicknames);
    const default_pool_id = ctx.memory.pool_router.pools_nicknames[_DEFAULT_POOL_NICKNAME];
    assert.ok(default_pool_id in ctx.memory.pool_router.pools);
  });

  t("router route by pool id returns added pool", () => {
    const routed = new _LAILA_IDENTIFIABLE_POOL();
    ctx.memory.extend(routed, { pool_nickname: "secondary" });
    assert.equal(ctx.memory.pool_router.route(["x"], { pool_id: routed.global_id }), routed);
  });

  t("router route by nickname returns added pool", () => {
    const routed = new _LAILA_IDENTIFIABLE_POOL();
    ctx.memory.extend(routed, { pool_nickname: "secondary" });
    assert.equal(ctx.memory.pool_router.route(["x"], { pool_nickname: "secondary" }), routed);
  });

  t("router pool id precedes pool nickname", () => {
    const pool_a = new _LAILA_IDENTIFIABLE_POOL();
    const pool_b = new _LAILA_IDENTIFIABLE_POOL();
    ctx.memory.extend(pool_a, { pool_nickname: "A" });
    ctx.memory.extend(pool_b, { pool_nickname: "B" });
    assert.equal(ctx.memory.pool_router.route(["x"], { pool_id: pool_a.global_id, pool_nickname: "B" }), pool_a);
  });

  t("record non batch dispatch calls non batch once", () => {
    const pool = new _LAILA_IDENTIFIABLE_POOL({ batch_accelerated: false });
    ctx.memory._parallel_individual_record = Mock("ok");
    ctx.memory._batch_accelerated_record = Mock("no");
    ctx.memory._record(["e1", "e2"], pool);
    ctx.memory._parallel_individual_record.assert_called_once_with(["e1", "e2"], pool);
    ctx.memory._batch_accelerated_record.assert_not_called();
  });

  t("fetch non batch dispatch calls non batch once", () => {
    const pool = new _LAILA_IDENTIFIABLE_POOL({ batch_accelerated: false });
    ctx.memory._parallel_individual_fetch = Mock("ok");
    ctx.memory._batch_accelerated_fetch = Mock("no");
    ctx.memory._fetch(["id1"], { pool });
    ctx.memory._parallel_individual_fetch.assert_called_once_with(["id1"], { pool });
    ctx.memory._batch_accelerated_fetch.assert_not_called();
  });

  t("delete non batch dispatch calls non batch once", () => {
    const pool = new _LAILA_IDENTIFIABLE_POOL({ batch_accelerated: false });
    ctx.memory._parallel_individual_delete = Mock("ok");
    ctx.memory._batch_accelerated_delete = Mock("no");
    ctx.memory._delete(["id1"], pool);
    ctx.memory._parallel_individual_delete.assert_called_once_with(["id1"], pool);
    ctx.memory._batch_accelerated_delete.assert_not_called();
  });

  t("remember single id is wrapped to list", () => {
    const routed = new _LAILA_IDENTIFIABLE_POOL();
    ctx.memory.extend(routed, { pool_nickname: "secondary" });
    ctx.memory._fetch = Mock("fetched");
    ctx.memory.remember("id1", { pool_id: routed.global_id, persist: false });
    assert.deepEqual(ctx.memory._fetch.call_args.args[0], ["id1"]);
  });

  t("remember set ids are wrapped to list", () => {
    const routed = new _LAILA_IDENTIFIABLE_POOL();
    ctx.memory.extend(routed, { pool_nickname: "secondary" });
    ctx.memory._fetch = Mock("fetched");
    ctx.memory.remember(new Set(["id1", "id2"]), { pool_id: routed.global_id, persist: false });
    const fetched_ids = ctx.memory._fetch.call_args.args[0];
    assert.ok(Array.isArray(fetched_ids) || fetched_ids instanceof Set);
    count_equal([...fetched_ids], ["id1", "id2"]);
  });

  t("forget single id is wrapped to list", () => {
    const routed = new _LAILA_IDENTIFIABLE_POOL();
    ctx.memory.extend(routed, { pool_nickname: "secondary" });
    ctx.memory._delete = Mock("deleted");
    ctx.memory.forget("id1", { pool_id: routed.global_id });
    assert.deepEqual(ctx.memory._delete.call_args.args[0], ["id1"]);
  });

  t("forget frozenset ids are wrapped to list", () => {
    const routed = new _LAILA_IDENTIFIABLE_POOL();
    ctx.memory.extend(routed, { pool_nickname: "secondary" });
    ctx.memory._delete = Mock("deleted");
    ctx.memory.forget(new Set(["id1", "id2"]), { pool_id: routed.global_id });
    const deleted_ids = ctx.memory._delete.call_args.args[0];
    assert.ok(Array.isArray(deleted_ids) || deleted_ids instanceof Set);
    count_equal([...deleted_ids], ["id1", "id2"]);
  });

  t("duplicate pool copies entries and returns group future", () => {
    const src_pool = new _LAILA_IDENTIFIABLE_POOL();
    const dest_pool = new _LAILA_IDENTIFIABLE_POOL();
    ctx.memory.extend(src_pool, { pool_nickname: "src" });
    ctx.memory.extend(dest_pool, { pool_nickname: "dest" });

    const entries = [laila.constant("alpha", { nickname: "dup-alpha" }), laila.constant({ x: 1 }, { nickname: "dup-beta" })];
    ctx.memory.memorize(entries, { pool_id: src_pool.global_id }).wait();

    const duplicate_futures = ctx.memory._duplicate_pool(src_pool, dest_pool, { inflight_max_entries: 1 });
    assert.ok(duplicate_futures instanceof GroupFuture);
    assert.equal(duplicate_futures.__len__(), 2);

    const duplicated_results = duplicate_futures.wait();
    count_equal(
      duplicated_results.map((r) => str(_unwrap(r))),
      entries.map((e) => e.global_id),
    );
    assert.equal(Number(laila.status(duplicate_futures).percentages.finished), 100.0);

    const remembered = ctx.memory.remember(
      entries.map((e) => e.global_id),
      { pool_id: dest_pool.global_id },
    ).wait();
    assert.deepEqual(
      remembered.map((e) => e.data),
      ["alpha", { x: 1 }],
    );
  });

  t("duplicate pool empty source returns empty group future", () => {
    const src_pool = new _LAILA_IDENTIFIABLE_POOL();
    const dest_pool = new _LAILA_IDENTIFIABLE_POOL();
    ctx.memory.extend(src_pool, { pool_nickname: "src-empty" });
    ctx.memory.extend(dest_pool, { pool_nickname: "dest-empty" });
    const duplicate_futures = ctx.memory._duplicate_pool(src_pool, dest_pool);
    assert.ok(duplicate_futures instanceof GroupFuture);
    assert.equal(duplicate_futures.__len__(), 0);
    assert.deepEqual(duplicate_futures.wait(), []);
  });

  t("duplicate pool inflight_max_entries less than one raises", () => {
    const src_pool = new _LAILA_IDENTIFIABLE_POOL();
    const dest_pool = new _LAILA_IDENTIFIABLE_POOL();
    ctx.memory.extend(src_pool, { pool_nickname: "src-invalid" });
    ctx.memory.extend(dest_pool, { pool_nickname: "dest-invalid" });
    assert.throws(() => ctx.memory._duplicate_pool(src_pool, dest_pool, { inflight_max_entries: 0 }), E.ValueError);
  });

  t("pool le operator duplicates source into destination", () => {
    const src_pool = new _LAILA_IDENTIFIABLE_POOL();
    const dest_pool = new _LAILA_IDENTIFIABLE_POOL();
    ctx.memory.extend(src_pool, { pool_nickname: "src-op" });
    ctx.memory.extend(dest_pool, { pool_nickname: "dest-op" });

    const entry = laila.constant("via-operator", { nickname: "dup-operator" });
    const bank = laila.get_active_policy().future_bank;
    const identity = ctx.memory.memorize(entry, { pool_id: src_pool.global_id });
    bank[identity.global_id].wait();

    const duplicate_futures = dest_pool.__le__(src_pool);
    assert.ok(duplicate_futures instanceof GroupFuture);
    const dup_ids = duplicate_futures.wait().map((r) => str(_unwrap(r)));
    assert.deepEqual(dup_ids, [entry.global_id]);

    const remember_identity = ctx.memory.remember(entry.global_id, { pool_id: dest_pool.global_id });
    assert.equal(bank[remember_identity.global_id].wait().data, "via-operator");
  });
});

// ---------------------------------------------------------------------------
// test_base_memory.py :: TestEvolutionAwareMemorizeRemember
// ---------------------------------------------------------------------------
/** In-memory pool that counts ``_keys`` scans (to assert index short-circuits). */
class _CountingPool extends _LAILA_IDENTIFIABLE_POOL {
  _keys(as_generator = false) {
    this._scan_count = (this._scan_count ?? 0) + 1;
    return super._keys(as_generator);
  }
}

/** Pool with its own ``_search_keys`` answer (an external index attachment). */
class _CustomIndexedPool extends _CountingPool {
  _search_keys(base_gid, _attributes) {
    const prefix = `${base_gid}@`;
    return with_(this.atomic(), () => Object.keys(this.resource).filter((k) => k === base_gid || k.startsWith(prefix)));
  }
}

describe("TestEvolutionAwareMemorizeRemember", () => {
  const ctx = {};
  use_test_policy(ctx);
  beforeEach(() =>
    macrotask(() => {
      ctx.pool = new _CountingPool();
      ctx.memory.extend(ctx.pool, { pool_nickname: "evo" });
    }),
  );

  const _memorize = (e) => ctx.memory.memorize(e, { pool_id: ctx.pool.global_id }).wait();
  const _remember = (ref) => ctx.memory.remember(ref, { pool_id: ctx.pool.global_id, persist: false }).wait();
  const _store_three_evolutions = () => {
    const v = laila.variable([0], { nickname: "evo-nick" });
    const stamps = [];
    _memorize(v);
    stamps.push(v.creation_timestamp);
    for (const i of [1, 2]) {
      time.sleep(0.002); // creation timestamps have millisecond precision
      v.data = [i];
      _memorize(v);
      stamps.push(v.creation_timestamp);
    }
    return [v, stamps];
  };
  const scans = () => ctx.pool._scan_count ?? 0;

  t("fresh variable is not locally modified and keeps its evolution", () => {
    const v = laila.variable([1], { evolution: 5 });
    assert.equal(v.locally_modified, false);
    _memorize(v);
    assert.equal(v.evolution, 5);
    assert.deepEqual([...ctx.pool.keys()], [v.global_id]);
  });

  t("memorize unchanged variable twice is idempotent", () => {
    const v = laila.variable([1]);
    _memorize(v);
    _memorize(v);
    assert.equal(v.evolution, 0);
    assert.equal([...ctx.pool.keys()].length, 1);
  });

  t("memorize after data reassignment bumps evolution", () => {
    const v = laila.variable([1]);
    _memorize(v);
    const hb0 = v.creation_timestamp;
    time.sleep(0.002);
    v.data = [2];
    assert.equal(v.locally_modified, true);
    _memorize(v);
    assert.equal(v.locally_modified, false);
    assert.equal(v.evolution, 1);
    assert.ok(v.global_id.endsWith("@evolution=1"));
    assert.notEqual(v.creation_timestamp, hb0);
    const keys = sorted(ctx.pool.keys());
    assert.equal(keys.length, 2);
    // The earlier evolution kept its own payload (no aliasing).
    assert.deepEqual(_remember(keys[0]).data, [1]);
    assert.deepEqual(_remember(keys[1]).data, [2]);
  });

  t("constant is never bumped", () => {
    const c = laila.constant(1);
    _memorize(c);
    c.data = 2;
    _memorize(c);
    assert.equal(c.evolution, null);
    assert.equal([...ctx.pool.keys()].length, 1);
    assert.equal(_remember(c.global_id).data, 2);
  });

  t("remembered entry starts clean", () => {
    const [v] = _store_three_evolutions();
    const r = _remember(v.global_id);
    assert.equal(r.locally_modified, false);
    _memorize(r);
    assert.equal(r.evolution, 2);
    assert.equal([...ctx.pool.keys()].length, 3);
    r.data = [99];
    _memorize(r);
    assert.equal(r.evolution, 3);
    assert.equal([...ctx.pool.keys()].length, 4);
  });

  t("remember without evolution returns highest", () => {
    _store_three_evolutions();
    const r = _remember("ENTRY:evo-nick");
    assert.equal(r.evolution, 2);
    assert.deepEqual(r.data, [2]);
  });

  t("remember with explicit evolution is exact", () => {
    _store_three_evolutions();
    const r = _remember("ENTRY:evo-nick@evolution=1");
    assert.equal(r.evolution, 1);
    assert.deepEqual(r.data, [1]);
  });

  t("remember by creation timestamp", () => {
    const [, stamps] = _store_three_evolutions();
    let r = _remember(`ENTRY:evo-nick@creation_timestamp=${stamps[1]}`);
    assert.equal(r.evolution, 1);
    assert.deepEqual(r.data, [1]);
    r = _remember(`ENTRY:evo-nick@evolution=2,creation_timestamp=${stamps[2]}`);
    assert.equal(r.evolution, 2);
  });

  t("remember creation timestamp mismatch raises key error", () => {
    const [, stamps] = _store_three_evolutions();
    assert.throws(() => _remember(`ENTRY:evo-nick@evolution=1,creation_timestamp=${stamps[2]}`), E.KeyError);
    assert.throws(() => _remember("ENTRY:evo-nick@creation_timestamp=1970-01-01T00:00:00.000+00:00"), E.KeyError);
  });

  t("remember unsupported attribute raises value error", () => {
    _store_three_evolutions();
    assert.throws(() => _remember("ENTRY:evo-nick@foo=bar"), E.ValueError);
  });

  t("remember missing variable raises key error", () => {
    assert.throws(() => _remember("ENTRY:never-stored"), E.KeyError);
  });

  t("remember prefers exact constant key", () => {
    const c = laila.constant("const", { nickname: "mixed" });
    _memorize(c);
    const r = _remember("ENTRY:mixed");
    assert.equal(r.evolution, null);
    assert.equal(r.data, "const");
  });

  t("highest evolution found through proxy chain", () => {
    const [v, stamps] = _store_three_evolutions();
    const front = new _LAILA_IDENTIFIABLE_POOL();
    front.__lshift__(ctx.pool);
    ctx.memory.extend(front, { pool_nickname: "front" });
    let r = ctx.memory.remember("ENTRY:evo-nick", { pool_id: front.global_id, persist: false }).wait();
    assert.equal(r.evolution, 2);
    // The winner is cached into the front tier, nothing else.
    assert.deepEqual([...front.keys()], [v.global_id]);
    r = ctx.memory.remember(`ENTRY:evo-nick@creation_timestamp=${stamps[0]}`, { pool_id: front.global_id, persist: false }).wait();
    assert.equal(r.evolution, 0);
  });

  t("indexed pool resolves without scanning", () => {
    const [, stamps] = _store_three_evolutions();
    const before = scans();
    assert.equal(_remember("ENTRY:evo-nick").evolution, 2);
    assert.equal(_remember("ENTRY:evo-nick@evolution=-1").evolution, 2);
    assert.equal(_remember("ENTRY:evo-nick@evolution=-2").evolution, 1);
    assert.equal(_remember(`ENTRY:evo-nick@creation_timestamp=${stamps[0]}`).evolution, 0);
    assert.equal(scans(), before);
  });

  t("index disabled pool falls back to scan", () => {
    const plain = new _CountingPool({ index_enabled: false });
    ctx.memory.extend(plain, { pool_nickname: "plain" });
    const v = laila.variable([0], { nickname: "plain-nick" });
    ctx.memory.memorize(v, { pool_id: plain.global_id }).wait();
    v.data = [1];
    ctx.memory.memorize(v, { pool_id: plain.global_id }).wait();
    assert.deepEqual([...plain.keys({ include_index: true })], sorted(plain.keys())); // no shards
    const before = plain._scan_count ?? 0;
    const r = ctx.memory.remember("ENTRY:plain-nick@evolution=-1", { pool_id: plain.global_id, persist: false }).wait();
    assert.equal(r.evolution, 1);
    assert.ok((plain._scan_count ?? 0) > before);
  });

  t("custom search keys hook short circuits scan", () => {
    const indexed = new _CustomIndexedPool({ index_enabled: false });
    ctx.memory.extend(indexed, { pool_nickname: "indexed" });
    const v = laila.variable([0], { nickname: "indexed-nick" });
    ctx.memory.memorize(v, { pool_id: indexed.global_id }).wait();
    v.data = [1];
    ctx.memory.memorize(v, { pool_id: indexed.global_id }).wait();
    const before = indexed._scan_count ?? 0;
    const r = ctx.memory.remember("ENTRY:indexed-nick", { pool_id: indexed.global_id, persist: false }).wait();
    assert.equal(r.evolution, 1);
    assert.equal(indexed._scan_count ?? 0, before);
  });

  t("negative evolution out of range raises", () => {
    _store_three_evolutions();
    assert.throws(() => _remember("ENTRY:evo-nick@evolution=-99"), E.KeyError);
    assert.throws(() => _remember("ENTRY:never-there@evolution=-1"), E.KeyError);
  });

  t("negative evolution with timestamp must agree", () => {
    const [, stamps] = _store_three_evolutions();
    const r = _remember(`ENTRY:evo-nick@evolution=-1,creation_timestamp=${stamps[2]}`);
    assert.equal(r.evolution, 2);
    assert.throws(() => _remember(`ENTRY:evo-nick@evolution=-1,creation_timestamp=${stamps[0]}`), E.KeyError);
  });

  t("stale index hit self heals", () => {
    const [v] = _store_three_evolutions();
    // Delete the latest evolution behind the index's back.
    ctx.pool._delete(v.global_id);
    const r = _remember("ENTRY:evo-nick@evolution=-1");
    assert.equal(r.evolution, 1);
    const base = v.global_id.split("@")[0];
    assert.deepEqual(ctx.pool.index.candidates(base), [`${base}@evolution=0`, `${base}@evolution=1`]);
  });

  t("forget negative evolution deletes latest", () => {
    const [v] = _store_three_evolutions();
    ctx.memory.forget("ENTRY:evo-nick@evolution=-1", { pool_id: ctx.pool.global_id }).wait();
    assert.equal(ctx.pool.exists(v.global_id), false);
    assert.equal(_remember("ENTRY:evo-nick").evolution, 1);
    // An evolution-less forget stays exact-key: nothing named exactly ``base``.
    ctx.memory.forget("ENTRY:evo-nick", { pool_id: ctx.pool.global_id }).wait();
    assert.equal(_remember("ENTRY:evo-nick").evolution, 1);
  });

  t("write back indexes front tier", () => {
    const [v] = _store_three_evolutions();
    const front = new _LAILA_IDENTIFIABLE_POOL();
    front.__lshift__(ctx.pool);
    ctx.memory.extend(front, { pool_nickname: "front2" });
    ctx.memory.remember("ENTRY:evo-nick", { pool_id: front.global_id, persist: false }).wait();
    const base = v.global_id.split("@")[0];
    assert.equal(front.index.latest(base), v.global_id);
  });

  t("duplicate pool does not copy index shards", () => {
    const [v] = _store_three_evolutions();
    const dest = new _LAILA_IDENTIFIABLE_POOL();
    ctx.memory.extend(dest, { pool_nickname: "dest-idx" });
    dest.__le__(ctx.pool).wait();
    assert.deepEqual(sorted(dest.keys()), sorted(ctx.pool.keys()));
    const src_shards = [...ctx.pool.keys({ include_index: true })].filter((k) => k.includes("POOL_INDEX"));
    const dest_shards = [...dest.keys({ include_index: true })].filter((k) => k.includes("POOL_INDEX"));
    assert.equal(src_shards.length, 1);
    assert.equal(dest_shards.length, 1);
    assert.notDeepEqual(src_shards, dest_shards); // dest has its *own* shard
    const base = v.global_id.split("@")[0];
    assert.equal(dest.index.latest(base), v.global_id);
  });
});

// ---------------------------------------------------------------------------
// test_alpha_pool_routing.py
// ---------------------------------------------------------------------------
const _TERMINAL = new Set(["finished", "error", "cancelled"]);

function _is_done(future) {
  const s = future.status;
  if (s !== null && typeof s === "object" && !("value" in s)) {
    const p = s.percentages ?? {};
    return (p.finished ?? 0) + (p.error ?? 0) + (p.cancelled ?? 0) >= 100.0;
  }
  return _TERMINAL.has(s.value);
}

/** Wait until all submitted tasks finish without killing the taskforce. */
function _flush(policy, timeout = 2.0) {
  const bank = policy.future_bank;
  const deadline = time.monotonic() + timeout;
  while (time.monotonic() < deadline) {
    if (Object.values(bank).every((f) => _is_done(f))) return;
    time.sleep(0.05);
  }
  throw new E.TimeoutError("Tasks did not finish in time");
}

describe("TestAlphaPoolRouting", () => {
  const ctx = {};
  use_test_policy(ctx);
  const alpha_pool = () => ctx.memory.pool_router.pools[ctx.memory.alpha_pool];

  t("alpha pool is set on default memory", () => assert.notEqual(ctx.memory.alpha_pool, null));
  t("alpha pool matches default nickname", () => {
    assert.equal(ctx.memory.alpha_pool, ctx.memory.pool_router.pools_nicknames[_DEFAULT_POOL_NICKNAME]);
  });
  t("router default route returns alpha pool", () => {
    assert.equal(ctx.memory.pool_router.route(["any"]), alpha_pool());
  });

  t("memorize no nickname returns future", () => {
    const result = ctx.memory.memorize(laila.constant(42));
    assert.notEqual(result, null);
    assert.ok(result instanceof _LAILA_IDENTIFIABLE_FUTURE);
  });
  t("memorize no nickname multiple returns group future", () => {
    assert.ok(ctx.memory.memorize([0, 1, 2].map((i) => laila.constant(i))) instanceof GroupFuture);
  });
  t("memorize no nickname records into alpha pool", () => {
    const entry = laila.constant("hello", { nickname: "alpha-memo-test" });
    ctx.memory.memorize(entry);
    _flush(ctx.policy);
    assert.ok(entry.global_id in alpha_pool().resource);
  });
  t("memorize no nickname calls record with alpha pool", () => {
    const entry = laila.constant("probe");
    const mock_record = Mock({});
    ctx.memory._record = mock_record;
    ctx.memory.memorize(entry);
    mock_record.assert_called_once();
    assert.equal(mock_record.call_args.args[1], alpha_pool());
  });
  t("memorize multiple entries all land in alpha pool", () => {
    const entries = [0, 1, 2, 3, 4].map((i) => laila.constant(i));
    ctx.memory.memorize(entries);
    _flush(ctx.policy);
    for (const entry of entries) assert.ok(entry.global_id in alpha_pool().resource);
  });

  t("remember no nickname returns future", () => {
    const entry = laila.constant("remember-me");
    ctx.memory.memorize(entry);
    _flush(ctx.policy);
    const result = ctx.memory.remember([entry.global_id]);
    assert.ok(result instanceof _LAILA_IDENTIFIABLE_FUTURE);
  });
  t("remember no nickname calls fetch with alpha pool", () => {
    const mock_fetch = Mock({});
    ctx.memory._fetch = mock_fetch;
    ctx.memory.remember(["some-id"]);
    mock_fetch.assert_called_once();
    assert.equal(mock_fetch.call_args.args[1].pool, alpha_pool());
  });
  t("remember no nickname recovers entry", () => {
    const entry = laila.constant({ key: "value" }, { nickname: "recall-test" });
    ctx.memory.memorize(entry);
    _flush(ctx.policy);
    const future_id = ctx.memory.remember([entry.global_id]);
    _flush(ctx.policy);
    const recalled = ctx.policy.future_bank[future_id.global_id].wait();
    assert.deepEqual(recalled.data, { key: "value" });
  });

  t("forget no nickname returns future", () => {
    assert.ok(ctx.memory.forget(["any-id"]) instanceof _LAILA_IDENTIFIABLE_FUTURE);
  });
  t("forget no nickname deletes from alpha pool", () => {
    const entry = laila.constant("to-be-forgotten");
    ctx.memory.memorize(entry);
    _flush(ctx.policy);
    assert.ok(entry.global_id in alpha_pool().resource);
    ctx.memory.forget(entry.global_id);
    _flush(ctx.policy);
    assert.ok(!(entry.global_id in alpha_pool().resource));
  });
  t("forget no nickname calls delete with alpha pool", () => {
    const mock_delete = Mock({});
    ctx.memory._delete = mock_delete;
    ctx.memory.forget(["id1"]);
    mock_delete.assert_called_once();
    const args = mock_delete.call_args.args;
    const used_pool = args[1] !== null && typeof args[1] === "object" && "pool" in args[1] ? args[1].pool : args[1];
    assert.equal(used_pool, alpha_pool());
  });

  t("end to end memorize remember forget in alpha pool", () => {
    const entries = ["a", "b", "c"].map((x) => laila.constant(x));
    ctx.memory.memorize(entries);
    _flush(ctx.policy);
    for (const e of entries) assert.ok(e.global_id in alpha_pool().resource);

    const recall_gf = ctx.memory.remember(entries.map((e) => e.global_id));
    _flush(ctx.policy);
    const recalled = ctx.policy.future_bank[recall_gf.global_id].wait();
    assert.deepEqual(sorted(recalled.map((r) => r.data)), ["a", "b", "c"]);

    ctx.memory.forget(entries.map((e) => e.global_id));
    _flush(ctx.policy);
    for (const e of entries) assert.ok(!(e.global_id in alpha_pool().resource));
  });

  t("memorize with explicit nickname skips alpha pool", () => {
    const secondary = new _LAILA_IDENTIFIABLE_POOL();
    ctx.memory.extend(secondary, { pool_nickname: "secondary" });
    const entry = laila.constant("not-alpha");
    const futures = ctx.memory.memorize(entry, { pool_nickname: "secondary" });
    assert.notEqual(futures, null);
    ctx.policy.future_bank[futures.global_id].wait();
    assert.ok(entry.global_id in secondary.resource);
    assert.ok(!(entry.global_id in alpha_pool().resource));
  });
  t("forget with explicit nickname skips alpha pool", () => {
    const secondary = new _LAILA_IDENTIFIABLE_POOL();
    ctx.memory.extend(secondary, { pool_nickname: "secondary" });
    const entry = laila.constant("secondary-data");
    const store_futures = ctx.memory.memorize(entry, { pool_nickname: "secondary" });
    ctx.policy.future_bank[store_futures.global_id].wait();
    const forget_futures = ctx.memory.forget(entry.global_id, { pool_nickname: "secondary" });
    assert.notEqual(forget_futures, null);
    ctx.policy.future_bank[forget_futures.global_id].wait();
    assert.ok(!(entry.global_id in secondary.resource));
  });
});

// ---------------------------------------------------------------------------
// test_manifest.py
// ---------------------------------------------------------------------------
describe("TestManifestConstruction", () => {
  test("001 from string dict sets blueprint", () => {
    const [gid_a, gid_b] = [_make_gid(), _make_gid()];
    const m = new Manifest({ data: { a: gid_a, b: gid_b } });
    assert.deepEqual(m.blueprint, { a: gid_a, b: gid_b });
  });
  test("002 from nested string dict", () => {
    const gid = _make_gid();
    const m = new Manifest({ data: { outer: { inner: gid } } });
    assert.equal(m.blueprint.outer.inner, gid);
  });
  test("003 from list of gids", () => {
    const gids = [_make_gid(), _make_gid(), _make_gid()];
    const m = new Manifest({ data: { batch: gids } });
    assert.deepEqual(m.blueprint.batch, gids);
  });
  test("004 from entry dict extracts blueprint", () => {
    const e1 = Entry.constant("hello");
    const e2 = Entry.constant("world");
    const m = new Manifest({ data: { e1, e2 } });
    assert.equal(m.blueprint.e1, e1.global_id);
    assert.equal(m.blueprint.e2, e2.global_id);
  });
  test("005 from entry list", () => {
    const entries = [0, 1, 2].map((i) => Entry.constant(i));
    const m = new Manifest({ data: { items: entries } });
    assert.deepEqual(
      m.blueprint.items,
      entries.map((e) => e.global_id),
    );
  });
  test("006 from nested entry dict", () => {
    const e = Entry.constant(42);
    const m = new Manifest({ data: { layer: { deep: e } } });
    assert.equal(m.blueprint.layer.deep, e.global_id);
  });
  test("007 mixed entries and strings raises", () => {
    assert.throws(() => new Manifest({ data: { entry: Entry.constant(1), string: _make_gid() } }), E.ValueError);
  });
  test("008 non string key raises", () => {
    // JS object keys are always strings; a ``Map`` carries the int key.
    assert.throws(() => new Manifest({ data: new Map([[42, _make_gid()]]) }), E.ValueError);
  });
  test("009 invalid value type raises", () => {
    assert.throws(() => new Manifest({ data: { bad: 123 } }), E.ValueError);
  });
  test("010 list with non string item raises", () => {
    assert.throws(() => new Manifest({ data: { bad_list: [_make_gid(), 42] } }), E.ValueError);
  });
  test("011 non dict blueprint raises", () => {
    assert.throws(() => Manifest._validate_blueprint("not a dict"), E.ValueError);
  });
});

describe("TestManifestIdentity", () => {
  test("012 has global id and manifest scope", () => {
    const m = new Manifest({ data: { k: _make_gid() } });
    assert.ok(m.global_id.includes("MANIFEST"));
    assert.deepEqual([...m.scopes], ["MANIFEST"]);
  });
  test("013 explicit uuid", () => {
    const raw_uuid = crypto.randomUUID();
    assert.equal(new Manifest({ data: { k: _make_gid() }, uuid: raw_uuid }).uuid, raw_uuid);
  });
  test("014 nickname produces deterministic uuid", () => {
    const m1 = new Manifest({ data: { k: _make_gid() }, nickname: "test_manifest" });
    const m2 = new Manifest({ data: { k: _make_gid() }, nickname: "test_manifest" });
    assert.equal(m1.uuid, m2.uuid);
  });
  test("015 str returns global id", () => {
    const m = new Manifest({ data: { k: _make_gid() } });
    assert.equal(str(m), m.global_id);
  });
  test("016 repr contains entry count", () => {
    const gids = [_make_gid(), _make_gid()];
    const r = repr(new Manifest({ data: { a: gids[0], b: gids[1] } }));
    assert.ok(r.includes("entries=2"));
    assert.ok(r.includes("Manifest("));
  });
  test("017 manifest is entry subclass", () => {
    assert.ok(new Manifest({ data: { k: _make_gid() } }) instanceof Entry);
  });
  test("018 manifest evolution is none", () => {
    assert.equal(new Manifest({ data: { k: _make_gid() } }).evolution, null);
  });
});

describe("TestManifestMappingAPI", () => {
  let gid_a, gid_b, m;
  beforeEach(() => {
    gid_a = _make_gid();
    gid_b = _make_gid();
    m = new Manifest({ data: { a: gid_a, b: gid_b } });
  });
  test("019 dict returns blueprint", () => {
    assert.deepEqual(Object.fromEntries(m.items()), { a: gid_a, b: gid_b });
  });
  test("020 len returns top level count", () => assert.equal(m.__len__(), 2));
  test("021 getitem returns value", () => assert.equal(m["a"], gid_a));
  test("022 getitem missing key raises", () => {
    assert.throws(() => m.__getitem__("missing"), E.KeyError);
    assert.equal(m["missing"], undefined);
  });
  test("023 keys values items", () => {
    count_equal(m.keys(), ["a", "b"]);
    count_equal(m.values(), [gid_a, gid_b]);
    assert.deepEqual(
      sorted(m.items().map((kv) => [...kv].join("="))),
      sorted([`a=${gid_a}`, `b=${gid_b}`]),
    );
  });
});

describe("TestManifestIteration", () => {
  test("024 iter flat dict", () => {
    const [gid_a, gid_b] = [_make_gid(), _make_gid()];
    assert.deepEqual([...new Manifest({ data: { a: gid_a, b: gid_b } })], [gid_a, gid_b]);
  });
  test("025 iter nested depth first", () => {
    const [gid_1, gid_2, gid_3] = [_make_gid(), _make_gid(), _make_gid()];
    const m = new Manifest({ data: { top: gid_1, nested: { inner: gid_2 }, last: gid_3 } });
    assert.deepEqual([...m], [gid_1, gid_2, gid_3]);
  });
  test("026 iter with lists", () => {
    const [gid_a, gid_b, gid_c] = [_make_gid(), _make_gid(), _make_gid()];
    const m = new Manifest({ data: { single: gid_a, batch: [gid_b, gid_c] } });
    assert.deepEqual([...m], [gid_a, gid_b, gid_c]);
  });
  test("027 contains finds leaf gid", () => {
    const gid = _make_gid();
    const m = new Manifest({ data: { nested: { deep: gid } } });
    assert.ok(gid in m);
    assert.ok(m.__contains__(gid));
  });
  test("028 contains returns false for missing", () => {
    assert.ok(!(_make_gid() in new Manifest({ data: { k: _make_gid() } })));
  });
  test("029 contains non string returns false", () => {
    assert.equal(new Manifest({ data: { k: _make_gid() } }).__contains__(42), false);
  });
});

describe("TestManifestEmptyState", () => {
  test("030 empty manifest len zero", () => {
    const m = new Manifest();
    assert.equal(m.__len__(), 0);
    assert.deepEqual([...m], []);
  });
  test("031 empty manifest keys values items", () => {
    const m = new Manifest();
    assert.deepEqual([...m.keys()], []);
    assert.deepEqual([...m.values()], []);
    assert.deepEqual([...m.items()], []);
  });
  test("032 empty manifest getitem raises", () => {
    assert.throws(() => new Manifest().__getitem__("anything"), E.KeyError);
  });
  test("033 remember on empty manifest raises", () => {
    assert.throws(() => new Manifest().remember(), E.RuntimeError);
  });
  test("034 memorize on empty manifest raises", () => {
    assert.throws(() => new Manifest().memorize(), E.RuntimeError);
  });
  test("035 forget on empty manifest raises", () => {
    assert.throws(() => new Manifest().forget(), E.RuntimeError);
  });
  test("036 resolved on empty manifest raises", () => {
    assert.throws(() => new Manifest().realized, E.RuntimeError);
  });
});

describe("TestManifestSubManifest", () => {
  let gid_a, gid_b, gid_c, m;
  beforeEach(() => {
    gid_a = _make_gid();
    gid_b = _make_gid();
    gid_c = _make_gid();
    m = new Manifest({ data: { sample_1: { mod_a: gid_a }, sample_2: { mod_b: gid_b }, sample_3: { mod_c: gid_c } } });
  });
  test("051 sub manifest returns manifest", () => assert.ok(m.sub_manifest(["sample_1"]) instanceof Manifest));
  test("052 sub manifest single key", () => {
    const sub = m.sub_manifest(["sample_2"]);
    assert.deepEqual([...sub.keys()], ["sample_2"]);
    assert.deepEqual(sub["sample_2"], { mod_b: gid_b });
  });
  test("053 sub manifest multiple keys", () => {
    const sub = m.sub_manifest(["sample_1", "sample_3"]);
    count_equal(sub.keys(), ["sample_1", "sample_3"]);
    assert.deepEqual(sub["sample_1"], { mod_a: gid_a });
    assert.deepEqual(sub["sample_3"], { mod_c: gid_c });
  });
  test("054 sub manifest all keys", () => {
    const sub = m.sub_manifest(["sample_1", "sample_2", "sample_3"]);
    assert.equal(sub.__len__(), 3);
    assert.deepEqual(sub.blueprint, m.blueprint);
  });
  test("055 sub manifest preserves nested structure", () => {
    const [gid_1, gid_2] = [_make_gid(), _make_gid()];
    const mm = new Manifest({ data: { sample_1: { mod_a: gid_1, mod_b: gid_2 }, sample_2: { mod_a: gid_1 } } });
    assert.deepEqual(mm.sub_manifest(["sample_1"])["sample_1"], { mod_a: gid_1, mod_b: gid_2 });
  });
  test("056 sub manifest preserves list leaves", () => {
    const gids = [_make_gid(), _make_gid()];
    const mm = new Manifest({ data: { sample_1: { batch: gids }, sample_2: { single: _make_gid() } } });
    assert.deepEqual(mm.sub_manifest(["sample_1"])["sample_1"], { batch: gids });
  });
  test("057 sub manifest missing key raises", () => {
    assert.throws(() => m.sub_manifest(["sample_1", "nonexistent"]), E.KeyError);
  });
  test("058 sub manifest empty manifest raises", () => {
    assert.throws(() => new Manifest().sub_manifest(["any_key"]), E.RuntimeError);
  });
  test("059 sub manifest is independent copy", () => {
    const sub = m.sub_manifest(["sample_1"]);
    sub["sample_1"]["mod_a"] = "MUTATED";
    assert.equal(m["sample_1"]["mod_a"], gid_a);
  });
  test("060 sub manifest has own identity", () => {
    assert.notEqual(m.sub_manifest(["sample_1"]).global_id, m.global_id);
  });
  test("061 sub manifest iterable", () => {
    count_equal([...m.sub_manifest(["sample_1", "sample_2"])], [gid_a, gid_b]);
  });
});

describe("TestManifestMemoryRoundtrip", () => {
  const ctx = {};
  use_test_policy(ctx, { register_local: true });
  beforeEach(() =>
    macrotask(() => {
      ctx.pool = new _LAILA_IDENTIFIABLE_POOL();
      laila.memory.extend(ctx.pool, { pool_nickname: "test_pool" });
    }),
  );

  t("037 memorize returns group future", () => {
    const m = new Manifest({ data: { k: Entry.constant("test") } });
    const future = m.memorize({ pool_nickname: "test_pool" });
    assert.ok(future instanceof GroupFuture);
    future.wait();
  });
  t("038 remember returns group future", () => {
    const m = new Manifest({ data: { k: Entry.constant("test") } });
    m.memorize({ pool_nickname: "test_pool" }).wait();
    const m2 = new Manifest({ data: m.blueprint });
    const future = m2.remember({ pool_nickname: "test_pool" });
    assert.ok(future instanceof GroupFuture);
    assert.equal(future.wait().length, 1);
  });
  t("039 forget returns group future", () => {
    const m = new Manifest({ data: { k: Entry.constant("test") } });
    m.memorize({ pool_nickname: "test_pool" }).wait();
    const future = m.forget({ pool_nickname: "test_pool" });
    assert.ok(future instanceof GroupFuture);
    future.wait();
  });
  t("040 memorize and resolved roundtrip", () => {
    const e1 = Entry.constant("alpha");
    const e2 = Entry.constant("beta");
    const m = new Manifest({ data: { a: e1, b: e2 } });
    m.memorize().wait();
    assert.notEqual(m.blueprint, null);
    assert.equal(m.blueprint.a, e1.global_id);
    const resolved = new Manifest({ data: m.blueprint }).realized;
    assert.equal(resolved.a.data, "alpha");
    assert.equal(resolved.b.data, "beta");
  });
  t("041 memorize and forget deletes entries", () => {
    const e = Entry.constant("ephemeral");
    const m = new Manifest({ data: { k: e } });
    m.memorize({ pool_nickname: "test_pool" }).wait();
    assert.equal(ctx.pool.exists(e.global_id), true);
    m.forget({ pool_nickname: "test_pool" }).wait();
    assert.equal(ctx.pool.exists(e.global_id), false);
  });
  t("042 memorize stores manifest itself", () => {
    const m = new Manifest({ data: { k: Entry.constant(99) }, nickname: "self_store_test" });
    m.memorize({ pool_nickname: "test_pool" }).wait();
    assert.equal(ctx.pool.exists(m.global_id), true);
  });
  t("043 forget deletes manifest itself", () => {
    const e = Entry.constant("keep_manifest");
    const m = new Manifest({ data: { k: e } });
    m.memorize({ pool_nickname: "test_pool" }).wait();
    assert.equal(ctx.pool.exists(m.global_id), true);
    m.forget({ pool_nickname: "test_pool" }).wait();
    assert.equal(ctx.pool.exists(e.global_id), false);
    assert.equal(ctx.pool.exists(m.global_id), false);
  });
  t("044 nested entry memorize resolved roundtrip", () => {
    const [e1, e2, e3] = ["layer1", "layer2", "layer3"].map((x) => Entry.constant(x));
    const m = new Manifest({ data: { top: e1, group: { nested: e2 }, batch: [e3] } });
    m.memorize({ pool_nickname: "test_pool" }).wait();
    const resolved = new Manifest({ data: m.blueprint }).remember({ pool_nickname: "test_pool" }).wait();
    assert.equal(resolved.length, 3);
    const realized = new Manifest({ data: m.blueprint }).realized;
    assert.equal(realized.top.data, "layer1");
    assert.equal(realized.group.nested.data, "layer2");
    assert.equal(realized.batch[0].data, "layer3");
  });
  t("045 batch size respected", () => {
    const entries = [0, 1, 2, 3, 4].map((i) => Entry.constant(i));
    const m = new Manifest({ data: { items: entries } });
    m.memorize({ pool_nickname: "test_pool", batch_size: 2 }).wait();
    for (const e of entries) assert.equal(ctx.pool.exists(e.global_id), true);
  });
  t("046 remember with pool nickname", () => {
    const m = new Manifest({ data: { k: Entry.constant("recall_me") } });
    m.memorize({ pool_nickname: "test_pool" }).wait();
    const results = new Manifest({ data: m.blueprint }).remember({ pool_nickname: "test_pool" }).wait();
    assert.equal(results.length, 1);
    assert.equal(results[0].data, "recall_me");
  });
});

describe("TestPoolWrapper", () => {
  const ctx = {};
  use_test_policy(ctx, { register_local: true });
  beforeEach(() =>
    macrotask(() => {
      ctx.src_pool = new _LAILA_IDENTIFIABLE_POOL();
      ctx.dest_pool = new _LAILA_IDENTIFIABLE_POOL();
      laila.memory.extend(ctx.src_pool, { pool_nickname: "src" });
      laila.memory.extend(ctx.dest_pool, { pool_nickname: "dest" });
    }),
  );

  t("047 pool getitem manifest returns pool wrapper", () => {
    const m = new Manifest({ data: { k: _make_gid() } });
    const wrapper = ctx.src_pool.__getitem__(m);
    assert.ok(wrapper instanceof PoolWrapper);
    assert.equal(wrapper.pool, ctx.src_pool);
    assert.equal(wrapper.manifest, m);
  });
  t("048 pool getitem string still works", () => {
    ctx.src_pool["test_key"] = "test_value";
    assert.equal(ctx.src_pool["test_key"], "test_value");
  });
  t("049 pool wrapper le copies manifest entries", () => {
    const e1 = Entry.constant("copy_me");
    const e2 = Entry.constant("copy_me_too");
    const m = new Manifest({ data: { a: e1, b: e2 } });
    m.memorize({ pool_id: ctx.src_pool.global_id }).wait();
    assert.equal(ctx.src_pool.exists(e1.global_id), true);
    assert.equal(ctx.dest_pool.exists(e1.global_id), false);

    const future = ctx.dest_pool.__getitem__(m).__le__(ctx.src_pool.__getitem__(m));
    assert.ok(future instanceof GroupFuture);
    future.wait();

    assert.equal(ctx.dest_pool.exists(e1.global_id), true);
    assert.equal(ctx.dest_pool.exists(e2.global_id), true);
  });
  t("050 pool wrapper le non wrapper returns NotImplemented", () => {
    const wrapper = ctx.src_pool.__getitem__(new Manifest({ data: { k: _make_gid() } }));
    assert.equal(wrapper.__le__("not a wrapper"), NotImplemented);
  });
});

describe("TestManifestCombine", () => {
  test("062 extend merges disjoint blueprints", () => {
    const [gid_a, gid_b] = [_make_gid(), _make_gid()];
    const m1 = new Manifest({ data: { a: gid_a } });
    m1.extend(new Manifest({ data: { b: gid_b } }));
    assert.deepEqual(m1.blueprint, { a: gid_a, b: gid_b });
  });
  test("063 extend raises on duplicate keys", () => {
    const m1 = new Manifest({ data: { a: _make_gid() } });
    assert.throws(() => m1.extend(new Manifest({ data: { a: _make_gid() } })), E.KeyError);
  });
  test("064 extend overwrite allows duplicate keys", () => {
    const [gid_old, gid_new] = [_make_gid(), _make_gid()];
    const m1 = new Manifest({ data: { a: gid_old } });
    m1.extend(new Manifest({ data: { a: gid_new } }), { overwrite: true });
    assert.equal(m1["a"], gid_new);
  });
  test("065 extend into empty manifest", () => {
    const gid = _make_gid();
    const m1 = new Manifest();
    m1.extend(new Manifest({ data: { k: gid } }));
    assert.deepEqual(m1.blueprint, { k: gid });
    assert.equal(m1.__len__(), 1);
  });
  test("066 extend with empty other is noop", () => {
    const gid = _make_gid();
    const m1 = new Manifest({ data: { a: gid } });
    m1.extend(new Manifest());
    assert.deepEqual(m1.blueprint, { a: gid });
  });
  test("067 extend non manifest raises type error", () => {
    assert.throws(() => new Manifest({ data: { a: _make_gid() } }).extend({ b: _make_gid() }), E.TypeError);
  });
  test("068 extend preserves nested structure", () => {
    const [gid_a, gid_b] = [_make_gid(), _make_gid()];
    const m1 = new Manifest({ data: { layer1: { inner: gid_a } } });
    m1.extend(new Manifest({ data: { layer2: { inner: gid_b } } }));
    assert.equal(m1["layer1"]["inner"], gid_a);
    assert.equal(m1["layer2"]["inner"], gid_b);
  });
  test("069 extend preserves list leaves", () => {
    const gids_a = [_make_gid(), _make_gid()];
    const gids_b = [_make_gid()];
    const m1 = new Manifest({ data: { batch_a: gids_a } });
    m1.extend(new Manifest({ data: { batch_b: gids_b } }));
    assert.deepEqual(m1["batch_a"], gids_a);
    assert.deepEqual(m1["batch_b"], gids_b);
  });
  test("070 extend combines pending entries", () => {
    const e1 = Entry.constant("one");
    const e2 = Entry.constant("two");
    const m1 = new Manifest({ data: { a: e1 } });
    m1.extend(new Manifest({ data: { b: e2 } }));
    assert.notEqual(m1._pending_entries, null);
    const pending = new Set(m1._pending_entries.map((e) => e.global_id));
    assert.ok(pending.has(e1.global_id));
    assert.ok(pending.has(e2.global_id));
  });
  test("071 extend deep copies other blueprint", () => {
    const gid = _make_gid();
    const m1 = new Manifest({ data: { a: _make_gid() } });
    const m2 = new Manifest({ data: { b: { nested: gid } } });
    m1.extend(m2);
    m2.blueprint["b"]["nested"] = "MUTATED";
    assert.equal(m1["b"]["nested"], gid);
  });
  test("072 iadd merges in place and returns self", () => {
    const [gid_a, gid_b] = [_make_gid(), _make_gid()];
    const m1 = new Manifest({ data: { a: gid_a } });
    const original_id = m1.global_id;
    const result = m1.__iadd__(new Manifest({ data: { b: gid_b } }));
    assert.equal(result, m1);
    assert.equal(m1.global_id, original_id);
    assert.deepEqual(m1.blueprint, { a: gid_a, b: gid_b });
  });
  test("073 iadd operator syntax", () => {
    const [gid_a, gid_b] = [_make_gid(), _make_gid()];
    let m1 = new Manifest({ data: { a: gid_a } });
    const before = m1;
    m1 = m1.__iadd__(new Manifest({ data: { b: gid_b } }));
    assert.equal(m1, before);
    assert.deepEqual(m1.blueprint, { a: gid_a, b: gid_b });
  });
  test("074 iadd raises on duplicate keys", () => {
    const m1 = new Manifest({ data: { a: _make_gid() } });
    assert.throws(() => m1.__iadd__(new Manifest({ data: { a: _make_gid() } })), E.KeyError);
  });
  test("075 iadd non manifest returns NotImplemented", () => {
    assert.equal(new Manifest({ data: { a: _make_gid() } }).__iadd__("not a manifest"), NotImplemented);
  });
  test("076 add returns new manifest", () => {
    const [gid_a, gid_b] = [_make_gid(), _make_gid()];
    const m3 = new Manifest({ data: { a: gid_a } }).__add__(new Manifest({ data: { b: gid_b } }));
    assert.ok(m3 instanceof Manifest);
    assert.deepEqual(m3.blueprint, { a: gid_a, b: gid_b });
  });
  test("077 add does not mutate originals", () => {
    const [gid_a, gid_b] = [_make_gid(), _make_gid()];
    const m1 = new Manifest({ data: { a: gid_a } });
    const m2 = new Manifest({ data: { b: gid_b } });
    void m1.__add__(m2);
    assert.deepEqual(m1.blueprint, { a: gid_a });
    assert.deepEqual(m2.blueprint, { b: gid_b });
  });
  test("078 add raises on duplicate keys", () => {
    assert.throws(() => new Manifest({ data: { a: _make_gid() } }).__add__(new Manifest({ data: { a: _make_gid() } })), E.KeyError);
  });
  test("079 add non manifest returns NotImplemented", () => {
    assert.equal(new Manifest({ data: { a: _make_gid() } }).__add__("not a manifest"), NotImplemented);
  });
  test("080 add new manifest has own identity", () => {
    const m1 = new Manifest({ data: { a: _make_gid() } });
    const m2 = new Manifest({ data: { b: _make_gid() } });
    const m3 = m1.__add__(m2);
    assert.notEqual(m3.global_id, m1.global_id);
    assert.notEqual(m3.global_id, m2.global_id);
  });
  test("081 add two empty manifests", () => {
    const m3 = new Manifest().__add__(new Manifest());
    assert.ok(m3 instanceof Manifest);
    assert.equal(m3.__len__(), 0);
  });
  test("082 add empty plus non empty", () => {
    const gid = _make_gid();
    assert.deepEqual(new Manifest().__add__(new Manifest({ data: { k: gid } })).blueprint, { k: gid });
  });
  test("083 add combines pending entries", () => {
    const e1 = Entry.constant("one");
    const e2 = Entry.constant("two");
    const m3 = new Manifest({ data: { a: e1 } }).__add__(new Manifest({ data: { b: e2 } }));
    assert.notEqual(m3._pending_entries, null);
    const pending = new Set(m3._pending_entries.map((e) => e.global_id));
    assert.ok(pending.has(e1.global_id));
    assert.ok(pending.has(e2.global_id));
  });
  test("084 add deep copies blueprints", () => {
    const gid = _make_gid();
    const m1 = new Manifest({ data: { a: { nested: gid } } });
    const m3 = m1.__add__(new Manifest({ data: { b: _make_gid() } }));
    m1.blueprint["a"]["nested"] = "MUTATED";
    assert.equal(m3["a"]["nested"], gid);
  });
});

// ---------------------------------------------------------------------------
// test_manifest_sql.py
// ---------------------------------------------------------------------------
/** Subclass emitting two rows per top-level key (nested shape). */
class _DoubleManifest extends Manifest {
  _sql_rows() {
    const blueprint = this.data ?? {};
    const rows = [];
    for (const [key, val] of Object.entries(blueprint)) {
      const owner = val !== null && typeof val === "object" ? (val.owner ?? null) : null;
      rows.push([[key, 0], { owner, half: 0 }]);
      rows.push([[key, 1], { owner, half: 1 }]);
    }
    return [rows, new Set(["owner", "half"])];
  }

  _sql_project(matched_keys, _select_items) {
    const blueprint = this.data ?? {};
    const top = [];
    const seen = new Set();
    for (const row_key of matched_keys) {
      const k = row_key[0];
      if (!seen.has(k)) {
        seen.add(k);
        top.push(k);
      }
    }
    const subset = Object.fromEntries(top.filter((k) => k in blueprint).map((k) => [k, deepcopy(blueprint[k])]));
    return new this.constructor({ blueprint: subset });
  }
}

/** Subclass whose _sql_rows accidentally yields an Entry in a row dict. */
class _EntryRowManifest extends Manifest {
  _sql_rows() {
    return [[["k", { bad: Entry.constant("x") }]], new Set(["bad"])];
  }
}

v8.setFlagsFromString("--expose-gc");
const _gc = vm.runInNewContext("gc");

/** ``del m; gc.collect()``: collect and let FinalizationRegistry callbacks run. */
async function gc_collect() {
  for (let i = 0; i < 20; i++) {
    _gc();
    await new Promise((r) => setTimeout(r, 10));
  }
}

function sql_suite(name, body) {
  describe(name, () => {
    const ctx = {};
    beforeEach(() => {
      ctx.orig_dirs = { ...LAILA_DEFAULT_DIRECTORIES };
      ctx.tmp_root = fs.mkdtempSync(path.join(os.tmpdir(), "laila_sql_test_"));
      laila.set_default_directory(ctx.tmp_root);
    });
    afterEach(() => {
      for (const k of Object.keys(LAILA_DEFAULT_DIRECTORIES)) delete LAILA_DEFAULT_DIRECTORIES[k];
      Object.assign(LAILA_DEFAULT_DIRECTORIES, ctx.orig_dirs);
      fs.rmSync(ctx.tmp_root, { recursive: true, force: true });
    });
    body(ctx);
  });
}

const keyset = (m) => new Set(m.keys());

sql_suite("TestManifestSqlBasics", (ctx) => {
  beforeEach(() => {
    ctx.m = new Manifest({ data: { a: { owner: "alice" }, b: { owner: "bob" }, c: { owner: "alice" } } });
  });
  test("select star with where", () => {
    const result = ctx.m.sql("SELECT * FROM ds WHERE owner = 'alice'");
    assert.ok(result instanceof Manifest);
    assert.deepEqual(keyset(result), new Set(["a", "c"]));
  });
  test("scalar filter single", () => {
    assert.deepEqual(keyset(ctx.m.sql("SELECT * FROM ds WHERE owner = 'bob'")), new Set(["b"]));
  });
  test("empty match returns empty same type", () => {
    const result = ctx.m.sql("SELECT * FROM ds WHERE owner = 'nobody'");
    assert.ok(result instanceof Manifest);
    assert.equal(result.__len__(), 0);
  });
  test("no where is identity copy fresh id", () => {
    const result = ctx.m.sql("SELECT * FROM ds");
    assert.deepEqual(keyset(result), new Set(["a", "b", "c"]));
    assert.notEqual(result.global_id, ctx.m.global_id);
  });
  test("multi item select ignores extras", () => {
    assert.deepEqual(keyset(ctx.m.sql("SELECT a, b FROM ds WHERE owner = 'alice'")), new Set(["a", "c"]));
  });
  test("double equals normalized", () => {
    assert.deepEqual(keyset(ctx.m.sql("SELECT * FROM ds WHERE owner == 'bob'")), new Set(["b"]));
  });
  test("alias prefixed select item", () => {
    assert.deepEqual(keyset(ctx.m.sql("SELECT ds.owner FROM ds WHERE owner = 'bob'")), new Set(["b"]));
  });
  test("returns fresh instance not mutating", () => {
    const before = deepcopy(ctx.m.blueprint);
    void ctx.m.sql("SELECT * FROM ds WHERE owner = 'alice'");
    assert.deepEqual(ctx.m.blueprint, before);
  });
});

sql_suite("TestManifestSqlErrors", (ctx) => {
  beforeEach(() => {
    ctx.m = new Manifest({ data: { a: { owner: "alice" } } });
  });
  test("unquoted rhs bareword raises helpful valueerror", () => {
    assert.throws(
      () => ctx.m.sql("SELECT * FROM ds WHERE owner = alice"),
      (e) => e instanceof E.ValueError && String(e).toLowerCase().includes("quote"),
    );
  });
  for (const [label, q] of [
    ["order by", "SELECT * FROM ds ORDER BY owner"],
    ["limit", "SELECT * FROM ds LIMIT 5"],
    ["group by", "SELECT * FROM ds GROUP BY owner"],
    ["having", "SELECT * FROM ds HAVING owner = 'alice'"],
    ["join", "SELECT * FROM ds JOIN other ON ds.x = other.x"],
    ["union", "SELECT * FROM ds UNION SELECT * FROM other"],
  ]) {
    test(`reject ${label}`, () => assert.throws(() => ctx.m.sql(q), E.ValueError));
  }
  test("non string query raises", () => assert.throws(() => ctx.m.sql(123), E.ValueError));
  test("malformed query raises", () => assert.throws(() => ctx.m.sql("DELETE FROM ds"), E.ValueError));
});

sql_suite("TestManifestSqlHeterogeneous", (ctx) => {
  beforeEach(() => {
    ctx.m = new Manifest({ data: { a: { owner: "alice", team: "x" }, b: { owner: "bob" } } });
  });
  test("missing column is null and queryable", () => {
    assert.deepEqual(keyset(ctx.m.sql("SELECT * FROM ds WHERE team = 'x'")), new Set(["a"]));
  });
  test("is null filter", () => {
    assert.deepEqual(keyset(ctx.m.sql("SELECT * FROM ds WHERE team IS NULL")), new Set(["b"]));
  });
});

sql_suite("TestManifestSqlIndexLifecycle", (ctx) => {
  beforeEach(() => {
    ctx.m = new Manifest({ data: { a: { owner: "alice" }, b: { owner: "bob" } } });
  });
  afterEach(() => ctx.m?.clear_index());
  test("build index on creates index", () => {
    ctx.m.build_index({ on: ["owner"] });
    assert.ok([...ctx.m._sql_state.indexed].includes("owner"));
    const index_names = Manifest._existing_indexes(ctx.m._sql_state.conn);
    assert.ok([...index_names].some((n) => n.includes("owner")));
  });
  test("default index path under indices dir", () => {
    ctx.m.build_index();
    assert.ok(ctx.m._sql_state.db_path.startsWith(LAILA_DEFAULT_DIRECTORIES.indices));
  });
  test("build index idempotent", () => {
    ctx.m.build_index();
    const state1 = ctx.m._sql_state;
    ctx.m.build_index();
    assert.equal(ctx.m._sql_state, state1);
  });
  test("mutation marks stale and rebuilds", () => {
    ctx.m.sql("SELECT * FROM ds WHERE owner = 'alice'");
    assert.equal(ctx.m._sql_state.stale, false);
    ctx.m.extend(new Manifest({ data: { c: { owner: "alice" } } }));
    assert.equal(ctx.m._sql_state.stale, true);
    const result = ctx.m.sql("SELECT * FROM ds WHERE owner = 'alice'");
    assert.deepEqual(keyset(result), new Set(["a", "c"]));
    assert.equal(ctx.m._sql_state.stale, false);
  });
  test("invalidate then sql rebuilds", () => {
    ctx.m.sql("SELECT * FROM ds");
    ctx.m.invalidate_index();
    assert.equal(ctx.m._sql_state.stale, true);
    ctx.m.sql("SELECT * FROM ds");
    assert.equal(ctx.m._sql_state.stale, false);
  });
  test("clear index resets state", () => {
    ctx.m.build_index();
    ctx.m.clear_index();
    assert.equal(ctx.m._sql_state, null);
    assert.deepEqual(keyset(ctx.m.sql("SELECT * FROM ds WHERE owner = 'bob'")), new Set(["b"]));
  });
});

sql_suite("TestManifestSqlPersistence", (ctx) => {
  test("persist roundtrip attaches without rebuild", () => {
    const p = path.join(ctx.tmp_root, "explicit", "idx.laila_sqlitedb");
    const blueprint = { a: { owner: "alice" }, b: { owner: "bob" } };
    const m1 = new Manifest({ data: deepcopy(blueprint) });
    m1.build_index({ on: ["owner"], persist: p });
    m1.clear_index(); // close connection, keep file
    assert.ok(fs.existsSync(p));

    const m2 = new Manifest({ data: deepcopy(blueprint) });
    m2.build_index({ persist: p }); // no `on=` -> attaches; "owner" index still present
    assert.ok([...m2._sql_state.indexed].some((n) => n.includes("owner")));
    assert.deepEqual(keyset(m2.sql("SELECT * FROM ds WHERE owner = 'alice'")), new Set(["a"]));
    m2.clear_index();
  });
  test("clear index remove persisted unlinks and rebuilds", () => {
    const p = path.join(ctx.tmp_root, "explicit2", "idx.laila_sqlitedb");
    const m = new Manifest({ data: { a: { owner: "alice" } } });
    m.build_index({ persist: p });
    assert.ok(fs.existsSync(p));
    m.clear_index({ remove_persisted: true });
    assert.ok(!fs.existsSync(p));
    assert.deepEqual(keyset(m.sql("SELECT * FROM ds WHERE owner = 'alice'")), new Set(["a"]));
    m.clear_index();
  });
});

sql_suite("TestManifestSqlGarbageCollection", (ctx) => {
  test("gc nukes temporary index file", async () => {
    let m = new Manifest({ data: { a: { owner: "alice" } } });
    m.build_index();
    const p = m._sql_state.db_path;
    assert.ok(fs.existsSync(p));
    m = null;
    await gc_collect();
    assert.ok(!fs.existsSync(p), "temporary index file should be removed when the manifest is GC'd");
  });
  test("gc nukes temporary index built via sql", async () => {
    let m = new Manifest({ data: { a: { owner: "alice" } } });
    m.sql("SELECT * FROM ds WHERE owner = 'alice'");
    const p = m._sql_state.db_path;
    assert.ok(fs.existsSync(p));
    m = null;
    await gc_collect();
    assert.ok(!fs.existsSync(p));
  });
  test("persistent index survives gc", async () => {
    const p = path.join(ctx.tmp_root, "keep", "idx.laila_sqlitedb");
    let m = new Manifest({ data: { a: { owner: "alice" } } });
    m.build_index({ persist: p });
    assert.ok(fs.existsSync(p));
    m = null;
    await gc_collect();
    assert.ok(fs.existsSync(p), "a user-chosen persist= file must NOT be nuked on GC");
  });
  test("clear index removes temporary file", () => {
    const m = new Manifest({ data: { a: { owner: "alice" } } });
    m.build_index();
    const p = m._sql_state.db_path;
    assert.ok(fs.existsSync(p));
    m.clear_index();
    assert.ok(!fs.existsSync(p));
  });
  test("clear index keeps persistent file by default", () => {
    const p = path.join(ctx.tmp_root, "keep2", "idx.laila_sqlitedb");
    const m = new Manifest({ data: { a: { owner: "alice" } } });
    m.build_index({ persist: p });
    m.clear_index(); // remove_persisted defaults to false
    assert.ok(fs.existsSync(p));
  });
});

sql_suite("TestManifestSqlSubclassHooks", () => {
  test("double manifest two rows per key", () => {
    const m = new _DoubleManifest({ data: { a: { owner: "alice" }, b: { owner: "bob" } } });
    const result = m.sql("SELECT * FROM ds WHERE half = 0 AND owner = 'alice'");
    assert.ok(result instanceof _DoubleManifest);
    assert.deepEqual(keyset(result), new Set(["a"]));
    m.clear_index();
  });
  test("double manifest dedupes top level keys", () => {
    const m = new _DoubleManifest({ data: { a: { owner: "alice" } } });
    // both rows for "a" match, but project keeps a single top-level key
    assert.deepEqual(keyset(m.sql("SELECT * FROM ds WHERE owner = 'alice'")), new Set(["a"]));
    m.clear_index();
  });
});

sql_suite("TestManifestSqlPrimitiveEnforcement", () => {
  test("entry in row raises typeerror no state", () => {
    const m = new _EntryRowManifest({ data: { k: _make_gid() } });
    assert.throws(
      () => m.build_index(),
      (e) => e instanceof E.TypeError && String(e).includes("global_id"),
    );
    assert.equal(m._sql_state, null);
  });
  test("list in row raises typeerror with flatten guidance", () => {
    const m = new Manifest({ data: { a: { tags: [_make_gid(), _make_gid()] } } });
    assert.throws(
      () => m.build_index(),
      (e) => e instanceof E.TypeError && String(e).includes("latten"),
    );
    assert.equal(m._sql_state, null);
  });
  test("gid string values pass", () => {
    const gid = _make_gid();
    const m = new Manifest({ data: { a: gid, b: _make_gid() } });
    assert.deepEqual(keyset(m.sql(`SELECT * FROM ds WHERE value = '${gid}'`)), new Set(["a"]));
    m.clear_index();
  });
});

sql_suite("TestManifestSqlMemoryUntouched", () => {
  // ``inspect.signature(...).parameters``: the JS methods take a trailing
  // options object; the index flags must not appear in their destructuring.
  const opts_of = (fn) => String(fn).split("\n").slice(0, 3).join("\n");
  test("memorize has no index flags", () => assert.ok(!opts_of(Manifest.prototype.memorize).includes("persist_index")));
  test("remember has no index flags", () => assert.ok(!opts_of(Manifest.prototype.remember).includes("load_index")));
  test("forget signature unchanged", () => {
    const sig = opts_of(Manifest.prototype.forget);
    assert.ok(!sig.includes("load_index"));
    assert.ok(!sig.includes("persist_index"));
  });
});

// ---------------------------------------------------------------------------
// test_manifest_direct_resolver.py
// ---------------------------------------------------------------------------
describe("TestDirectResolver", () => {
  const _make_manifest = (n) => {
    const leaves = Array.from({ length: n }, (_, i) => Entry.constant(i));
    const m = new Manifest({ data: Object.fromEntries(leaves.map((e, i) => [`k${i}`, e])) });
    const ref = m.memorize();
    ref.wait(_T);
    ref.release();
    return [m, (n * (n - 1)) / 2];
  };
  const sum_data = (d) => Object.values(d).reduce((a, e) => a + e.data, 0);

  beforeEach(() => macrotask(() => void laila.get_active_policy()));

  t("001 realized uses one future and releases it", () => {
    const [m, expected] = _make_manifest(200);
    const before = Object.keys(_bank()).length;
    assert.equal(sum_data(m.realized), expected);
    // The single batch future was released; no per-child futures exist.
    assert.equal(Object.keys(_bank()).length, before);
  });

  t("002 async realized from foreign loop", () => {
    const [m, expected] = _make_manifest(50);
    const before = Object.keys(_bank()).length;
    const out = laila.command.submit([async () => sum_data(await m.async_realized)]);
    const total = out.wait(_T).data;
    out.release();
    assert.equal(total, expected);
    assert.equal(Object.keys(_bank()).length, before);
  });

  t("003 async realized inside taskforce slot creates no futures", () => {
    const [m, expected] = _make_manifest(50);
    const _task = async () => {
      const bank_before = Object.keys(_bank()).length;
      const d = await m.async_realized;
      // Direct path: no future was created for the batch at all.
      const bank_after = Object.keys(_bank()).length;
      return [sum_data(d), bank_after - bank_before];
    };
    const fut = laila.command.submit([_task]);
    const [total, delta] = fut.wait(_T).data;
    fut.release();
    assert.equal(total, expected);
    assert.equal(delta, 0);
  });

  t("004 read entries async preserves order and types", () => {
    const [m] = _make_manifest(30);
    const gids = [...m];
    const memory = laila.get_active_policy().central.memory;
    const fut = laila.command.submit([async () => memory._read_entries_async(gids)]);
    const entries = fut.wait(_T).data;
    fut.release();
    assert.deepEqual(
      entries.map((e) => e.global_id),
      gids,
    );
    assert.ok(entries.every((e) => e instanceof Entry));
    assert.ok(entries.every((e) => e.state === EntryState.READY));
  });

  t("005 missing entry raises keyerror", () => {
    const [m] = _make_manifest(3);
    const missing = Entry.constant(999).global_id;
    const broken = new Manifest({ data: { a: [...m][0], b: missing } });
    assert.throws(() => broken.realized, E.KeyError);
  });

  t("006 persist caches into alpha pool", () => {
    const memory = laila.get_active_policy().central.memory;
    const secondary = new laila.DefaultPool();
    memory.extend(secondary, { pool_nickname: "direct_secondary" });
    const alpha = memory.pool_router.pools[memory.alpha_pool];
    const e = Entry.constant("side");
    const ref = laila.memorize(e, { dst_pool: "direct_secondary" });
    ref.wait(_T);
    ref.release();
    assert.ok(!(e.global_id in alpha.resource));

    let fut = laila.command.submit([async () => memory._read_entries_async([e.global_id], { pool: "direct_secondary" })]);
    const out = fut.wait(_T).data;
    fut.release();
    assert.equal(out[0].data, "side");
    assert.ok(e.global_id in alpha.resource);

    // persist=False path still resolves.
    fut = laila.command.submit([async () => memory._read_entries_async([e.global_id], { pool: "direct_secondary", persist: false })]);
    assert.equal(fut.wait(_T).data[0].data, "side");
    fut.release();
  });

  t("007 direct resolver checks the resolve chain", () => {
    const [m] = _make_manifest(3);
    const gids = [...m];
    const memory = laila.get_active_policy().central.memory;

    const _go = async () => {
      // Pretend gids[1] is already being resolved up the causal chain.
      const token = _RESOLVE_CHAIN.set([gids[1]]);
      try {
        return await memory._read_entries_async(gids);
      } finally {
        _RESOLVE_CHAIN.reset(token);
      }
    };
    let fut = laila.command.submit([_go]);
    assert.throws(() => fut.wait(_T), CyclicDependencyError);
    fut.release();

    // And the chain is extended per child while reading: a clean read works.
    fut = laila.command.submit([async () => memory._read_entries_async(gids)]);
    assert.equal(fut.wait(_T).data.length, 3);
    fut.release();
  });

  t("008 realized still works inside sync constitution body", () => {
    // A constitution body calling manifest.realized -> _read_entries_direct
    // (internal -> internal submit is rank-legal) must complete and be released.
    const [m, expected] = _make_manifest(40);
    const src = "function combine(manifest) {\n  let s = 0;\n  for (const e of Object.values(manifest.realized)) s += e.data;\n  return s;\n}\n";
    const v = Entry.variable(null, { constitution: src, manifest: m });
    const before = Object.keys(_bank()).length;
    const fut = laila.build(v);
    const out = fut.wait(_T);
    fut.release();
    assert.equal(out.data, expected);
    assert.equal(v.state, EntryState.READY);
    assert.equal(Object.keys(_bank()).length, before);
  });
});

// Tear down the lazily-activated default policy so the process exits.
test("zz teardown", () =>
  macrotask(() => {
    laila.terminate();
    fs.rmSync(TMP_ROOT, { recursive: true, force: true });
  }));
