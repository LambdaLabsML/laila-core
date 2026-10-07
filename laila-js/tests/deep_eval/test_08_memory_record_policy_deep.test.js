/**
 * Port of ``tests/deep_eval/test_08_memory_record_policy_deep.py``.
 *
 * Deep black-box tests for central memory (memorize / remember / forget),
 * Record provenance, cache-back persistence, multi-policy routing and the
 * Policy convenience API.
 */
import { test, describe } from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { S, laila, macrotask, with_fresh_policy_async as with_fresh_policy } from "./_fixtures.js";

const E = await import(S + "_compat/errors.js");
const T = await import(S + "_compat/pytypes.js");
const TH = await import(S + "_compat/threading.js");
const { with_ } = await import(S + "_compat/contextlib.js");
const { SQLitePool } = await import(S + "data/sqlite/sqlite.js");
const { TransformationSequence } = await import(S + "entry/compdata/transformation/base.js");
const { Base64 } = await import(S + "entry/compdata/transformation/base64/base64.js");
const { Entry } = await import(S + "entry/entry.js");
const { EntryState } = await import(S + "entry/entry_state.js");
const { DefaultPool } = await import(S + "macros/defaults.js");
const { GroupFuture } = await import(S + "policy/central/command/schema/future/future/group_future.js");
const { Record } = await import(S + "policy/central/memory/record/record.js");
const { _LAILA_IDENTIFIABLE_CENTRAL_MEMORY } = await import(S + "policy/central/memory/schema/base.js");
const { _LAILA_IDENTIFIABLE_POLICY } = await import(S + "policy/schema/base.js");
const { get_logger } = await import(S + "logger/index.js");

const { dict_has, dict_keys, getitem } = T;

const U1 = "11111111-2222-3333-4444-555555555555";

const TMP_ROOT = fs.mkdtempSync(path.join(os.tmpdir(), "laila_deep08_"));
laila.set_default_directory(TMP_ROOT);

const tp = (name, fn) => test(name, () => with_fresh_policy(fn));
const tmp_path = () => fs.mkdtempSync(path.join(TMP_ROOT, "tmp_"));
const range = (n) => Array.from({ length: n }, (_, i) => i);
const sorted = (xs) => [...xs].sort();

/** ``sq`` fixture: a SQLite pool registered as ``"sq"`` on ``fresh_policy``. */
const tsq = (name, fn) =>
  tp(name, (fresh_policy) => {
    const p = new SQLitePool({ file_path: path.join(tmp_path(), "sq.sqlite") });
    fresh_policy.central.memory.extend(p, { pool_nickname: "sq" });
    try {
      return fn(p, fresh_policy);
    } finally {
      p.close();
    }
  });

// ---------------------------------------------------------------------------
// Record
// ---------------------------------------------------------------------------

describe("TestRecord", () => {
  tp("test_defaults", (fresh_policy) => {
    const e = Entry.constant(1);
    const r = new Record({ entry: e });
    assert.equal(r.entry, e);
    assert.equal(r.recorder, fresh_policy.global_id);
    assert.equal(r.borrower, null);
    assert.ok(r.record_timestamp.endsWith("+00:00"));
  });

  tp("test_explicit_recorder", () => {
    const r = new Record({ entry: Entry.constant(1), recorder: "LAILA:POLICY:" + U1, borrower: "b" });
    assert.equal(r.recorder, "LAILA:POLICY:" + U1);
    assert.equal(r.borrower, "b");
  });

  tp("test_borrower_kept_when_recorder_defaulted", (fresh_policy) => {
    const r = new Record({ entry: Entry.constant(1), borrower: "b" });
    assert.equal(r.borrower, "b");
    assert.equal(r.recorder, fresh_policy.global_id);
  });

  tp("test_entry_id", () => {
    const e = Entry.constant(1, { uuid: U1 });
    assert.equal(new Record({ entry: e }).entry_id, e.global_id);
    assert.equal(new Record({ entry: { _global_id: "X" } }).entry_id, "X");
  });

  tp("test_entry_id_invalid_type_error", () => {
    assert.throws(
      () => new Record({ entry: 5 }).entry_id,
      (e) => e instanceof E.TypeError || e instanceof E.ValueError,
    );
  });

  tp("test_entry_id_error_message_names_type", () => {
    assert.throws(
      () => new Record({ entry: 5 }).entry_id,
      (e) => e instanceof E.TypeError && /int/.test(e.message),
    );
  });

  tp("test_from_dict_requires_entry", () => {
    assert.throws(() => Record.from_dict({}), E.ValueError);
    assert.throws(() => Record.from_dict("nope"), E.TypeError);
  });

  tp("test_from_dict_roundtrip_live_entry", () => {
    const e = Entry.constant(1);
    const r = new Record({ entry: e, borrower: "b" });
    const r2 = Record.from_dict(r.as_dict);
    assert.equal(r2.entry, e);
    assert.equal(r2.recorder, r.recorder);
    assert.equal(r2.borrower, "b");
    assert.equal(r2.record_timestamp, r.record_timestamp);
  });

  tp("test_from_dict_rebuilds_entry_dict", () => {
    const e = Entry.constant({ k: 1 }, { uuid: U1 });
    const d = new Record({ entry: e }).as_dict;
    d.entry = e.as_dict();
    const r = Record.from_dict(d);
    assert.equal(r.entry.global_id, e.global_id);
    assert.deepEqual(r.entry.data, { k: 1 });
  });

  tp("test_as_dict", () => {
    const e = Entry.constant(1);
    const d = new Record({ entry: e }).as_dict;
    assert.equal(d.entry, e);
    const keys = new Set(dict_keys(d));
    for (const k of ["entry", "recorder", "borrower", "record_timestamp"]) assert.ok(keys.has(k), k);
  });

  tp("test_serialize_embeds_entry_dict", () => {
    const e = Entry.constant([1], { uuid: U1 });
    const d = new Record({ entry: e }).serialize(new TransformationSequence({ transformations: [new Base64()] }));
    assert.equal(d.entry._uuid, U1);
    assert.equal(typeof d.entry.payload, "string");
  });

  tp("test_serialize_none_transformations_keeps_entry", () => {
    const e = Entry.constant([1]);
    const d = new Record({ entry: e }).serialize(null);
    assert.equal(d.entry, e);
  });

  tp("test_build_sync_roundtrip", (fresh_policy) => {
    const e = Entry.variable({ a: 1 }, { uuid: U1 });
    const d = new Record({ entry: e }).serialize(new TransformationSequence());
    const out = Record._build_sync(d);
    assert.deepEqual(getitem(out, "entry").data, { a: 1 });
    assert.equal(getitem(out, "entry").global_id, e.global_id);
    assert.equal(getitem(out, "recorder"), fresh_policy.global_id);
  });

  tp("test_build_async_roundtrip", async () => {
    const e = Entry.variable({ a: 1 }, { uuid: U1 });
    const d = new Record({ entry: e }).serialize(new TransformationSequence());
    const out = await Record._build_async(d);
    assert.deepEqual(getitem(out, "entry").data, { a: 1 });
  });

  tp("test_record_rejects_unknown_fields", () => {
    assert.throws(() => new Record({ entry: Entry.constant(1), creator: "x" }));
  });
});

// ---------------------------------------------------------------------------
// memorize / remember / forget on the alpha pool
// ---------------------------------------------------------------------------

describe("TestAlphaPoolLifecycle", () => {
  tp("test_memorize_single_returns_future_with_gid", () => {
    const e = Entry.constant(1);
    const f = laila.memorize(e);
    assert.equal(f.data, e.global_id);
  });

  tp("test_memorize_many_returns_group", () => {
    const es = range(3).map((i) => Entry.constant(i));
    const g = laila.memorize(es);
    assert.ok(g instanceof GroupFuture);
    assert.deepEqual(
      g.data,
      es.map((e) => e.global_id),
    );
  });

  tp("test_remember_roundtrip", () => {
    const e = Entry.constant({ a: 1 });
    laila.memorize(e).wait(10);
    const r = laila.remember(e.global_id);
    assert.deepEqual(r.result.data, { a: 1 });
    assert.deepEqual(r.data, { a: 1 });
  });

  tp("test_remember_many", () => {
    const es = range(5).map((i) => Entry.constant(i));
    laila.memorize(es).wait(10);
    const g = laila.remember(es.map((e) => e.global_id));
    assert.deepEqual(g.data, range(5));
  });

  tp("test_remember_missing_raises_keyerror", () => {
    assert.throws(() => laila.remember(`LAILA:ENTRY:${U1}`).wait(10), E.KeyError);
  });

  tp("test_forget", () => {
    const e = Entry.constant(1);
    laila.memorize(e).wait(10);
    laila.forget(e.global_id).wait(10);
    assert.throws(() => laila.remember(e.global_id).wait(10), E.KeyError);
  });

  tp("test_forget_missing_is_noop", () => {
    laila.forget(`LAILA:ENTRY:${U1}`).wait(10);
  });

  tp("test_forget_many", () => {
    const es = range(3).map((i) => Entry.constant(i));
    laila.memorize(es).wait(10);
    laila.forget(es.map((e) => e.global_id)).wait(10);
    for (const e of es) assert.throws(() => laila.remember(e.global_id).wait(10), E.KeyError);
  });

  tp("test_nickname_roundtrip", () => {
    const e = Entry.constant(7, { nickname: "seven" });
    laila.memorize(e).wait(10);
    assert.equal(laila.remember({ nickname: "seven" }).data, 7);
    assert.equal(laila.remember("seven").data, 7);
    assert.equal(laila.remember("ENTRY:seven").data, 7);
  });

  tp("test_forget_by_nickname", () => {
    const e = Entry.constant(7, { nickname: "seven2" });
    laila.memorize(e).wait(10);
    laila.forget({ nickname: "seven2" }).wait(10);
    assert.throws(() => laila.remember({ nickname: "seven2" }).wait(10), E.KeyError);
  });

  tp("test_variable_bump_on_modified_memorize", () => {
    const v = Entry.variable(1);
    laila.memorize(v).wait(10);
    v.data = 2;
    laila.memorize(v).wait(10);
    assert.equal(v.evolution, 1);
    const base = v.global_id.split("@")[0];
    assert.equal(laila.remember(`${base}@evolution=0`).data, 1);
    assert.equal(laila.remember(`${base}@evolution=1`).data, 2);
    assert.equal(laila.remember(`${base}@evolution=-1`).data, 2);
  });

  tp("test_variable_no_bump_without_modification", () => {
    const v = Entry.variable(1);
    laila.memorize(v).wait(10);
    laila.memorize(v).wait(10);
    assert.equal(v.evolution, 0);
  });

  tp("test_alpha_pool_snapshot_isolation", () => {
    const v = Entry.variable([1]);
    laila.memorize(v).wait(10);
    v.data = [2];
    laila.memorize(v).wait(10);
    const base = v.global_id.split("@")[0];
    assert.deepEqual(laila.remember(`${base}@evolution=0`).data, [1]);
  });

  tp("test_evolve_and_memorize_chain", () => {
    let v = Entry.variable(0);
    const versions = [v];
    for (let i = 1; i < 5; i++) {
      v = v.evolve(i);
      versions.push(v);
    }
    laila.memorize(versions).wait(10);
    const base = v.global_id.split("@")[0];
    assert.equal(laila.remember(`${base}@evolution=-1`).data, 4);
    assert.equal(laila.remember(`${base}@evolution=-5`).data, 0);
  });

  tp("test_staged_entry_cannot_be_memorized", () => {
    // ``def f(x): return 1`` -> the JS constitution language.
    const e = Entry.contingent({ constitution: ["function f(x) { return 1; }"] });
    assert.throws(() => laila.memorize(e).wait(10), E.RuntimeError);
  });

  tp("test_remember_returns_same_object_from_alpha", () => {
    // In-memory alpha pool stores constants by reference.
    const e = Entry.constant([1]);
    laila.memorize(e).wait(10);
    assert.equal(laila.remember(e.global_id).result, e);
  });

  tp("test_remember_variable_from_alpha_is_snapshot", () => {
    const v = Entry.variable([1]);
    laila.memorize(v).wait(10);
    const out = laila.remember(v.global_id).result;
    assert.notEqual(out, v);
    assert.equal(out.global_id, v.global_id);
  });

  tp("test_remember_sets_locally_modified_false", () => {
    const v = Entry.variable([1]);
    laila.memorize(v).wait(10);
    assert.equal(v.locally_modified, false);
  });

  tp("test_invalid_reference_raises_immediately", () => {
    assert.throws(() => laila.remember("LAILA:"), E.ValueError);
  });

  tp("test_memorize_non_entry_rejected", () => {
    assert.throws(() => laila.memorize("not an entry").wait(10));
  });

  tp("test_guarantee_block", () => {
    const e = Entry.constant(1);
    with_(laila.guarantee, () => {
      laila.memorize(e);
    });
    assert.equal(laila.remember(e.global_id).data, 1);
  });

  tp("test_alpha_pool_property", (fresh_policy) => {
    const ap = laila.alpha_pool;
    assert.equal(ap.global_id, fresh_policy.central.memory.alpha_pool);
  });

  tp("test_memory_property", (fresh_policy) => {
    assert.equal(laila.memory, fresh_policy.central.memory);
  });
});

// ---------------------------------------------------------------------------
// Non-alpha pools, routing, persist cache-back
// ---------------------------------------------------------------------------

describe("TestRoutingAndPersist", () => {
  tsq("test_memorize_to_nickname", (sq) => {
    const e = Entry.constant({ a: 1 });
    laila.memorize(e, { pool_nickname: "sq" }).wait(10);
    assert.ok(sq.keys().includes(e.global_id));
    assert.ok(!laila.alpha_pool.keys().includes(e.global_id));
  });

  tsq("test_memorize_to_pool_id", (sq) => {
    const e = Entry.constant(1);
    laila.memorize(e, { pool_id: sq.global_id }).wait(10);
    assert.ok(sq.keys().includes(e.global_id));
  });

  tp("test_unknown_nickname_raises", () => {
    assert.throws(() => laila.memorize(Entry.constant(1), { pool_nickname: "nope" }), E.KeyError);
  });

  tsq("test_remember_persist_caches_into_alpha", () => {
    const e = Entry.constant({ a: 1 });
    laila.memorize(e, { pool_nickname: "sq" }).wait(10);
    const out = laila.remember(e.global_id, { pool_nickname: "sq" });
    assert.deepEqual(out.data, { a: 1 });
    assert.ok(laila.alpha_pool.keys().includes(e.global_id));
    // second read served from alpha even without nickname
    assert.deepEqual(laila.remember(e.global_id).data, { a: 1 });
  });

  tsq("test_remember_persist_false_does_not_cache", () => {
    const e = Entry.constant({ a: 1 });
    laila.memorize(e, { pool_nickname: "sq" }).wait(10);
    assert.deepEqual(laila.remember(e.global_id, { pool_nickname: "sq", persist: false }).data, { a: 1 });
    assert.ok(!laila.alpha_pool.keys().includes(e.global_id));
  });

  tsq("test_remember_many_with_persist", () => {
    const es = range(4).map((i) => Entry.constant(i));
    laila.memorize(es, { pool_nickname: "sq" }).wait(10);
    const g = laila.remember(
      es.map((e) => e.global_id),
      { pool_nickname: "sq" },
    );
    assert.deepEqual(g.data, [0, 1, 2, 3]);
    for (const e of es) assert.ok(laila.alpha_pool.keys().includes(e.global_id));
  });

  tsq("test_persisted_variable_latest_resolution", () => {
    const v = Entry.variable(1);
    const v2 = v.evolve(2);
    laila.memorize([v, v2], { pool_nickname: "sq" }).wait(10);
    const base = v.global_id.split("@")[0];
    const out = laila.remember(`${base}@evolution=-1`, { pool_nickname: "sq" });
    assert.equal(out.data, 2);
    assert.ok(laila.alpha_pool.keys().includes(v2.global_id));
  });

  tsq("test_remember_from_alpha_misses_pool_specific_entry", () => {
    const e = Entry.constant(1);
    laila.memorize(e, { pool_nickname: "sq" }).wait(10);
    assert.throws(() => laila.remember(e.global_id).wait(10), E.KeyError);
  });

  tsq("test_forget_from_named_pool", (sq) => {
    const e = Entry.constant(1);
    laila.memorize(e, { pool_nickname: "sq" }).wait(10);
    laila.forget(e.global_id, { pool_nickname: "sq" }).wait(10);
    assert.ok(!sq.keys().includes(e.global_id));
  });

  tsq("test_serialized_roundtrip_through_sqlite_preserves_identity", () => {
    const v = Entry.variable({ k: [1, 2] }, { uuid: U1, evolution: 3 });
    laila.memorize(v, { pool_nickname: "sq" }).wait(10);
    const out = laila.remember(v.global_id, { pool_nickname: "sq", persist: false }).result;
    assert.equal(out.global_id, v.global_id);
    assert.equal(out.creation_timestamp, v.creation_timestamp);
    assert.equal(out.state, EntryState.READY);
    assert.equal(out.locally_modified, false);
    assert.notEqual(out, v);
  });

  tp("test_proxy_chain_read_through_memory", (fresh_policy) => {
    const origin = new SQLitePool({ file_path: path.join(tmp_path(), "o.sqlite") });
    const cache = new DefaultPool();
    cache.__lshift__(origin);
    fresh_policy.central.memory.extend(origin, { pool_nickname: "origin" });
    fresh_policy.central.memory.extend(cache, { pool_nickname: "cache" });
    const e = Entry.constant(5);
    laila.memorize(e, { pool_nickname: "origin" }).wait(10);
    assert.equal(laila.remember(e.global_id, { pool_nickname: "cache", persist: false }).data, 5);
    assert.ok(cache.keys().includes(e.global_id));
    origin.close();
  });

  tp("test_proxy_chain_negative_evolution_resolves_upstream", (fresh_policy) => {
    const origin = new SQLitePool({ file_path: path.join(tmp_path(), "o2.sqlite") });
    const cache = new DefaultPool();
    cache.__lshift__(origin);
    fresh_policy.central.memory.extend(origin, { pool_nickname: "origin2" });
    fresh_policy.central.memory.extend(cache, { pool_nickname: "cache2" });
    const v = Entry.variable(1);
    const v2 = v.evolve(2);
    laila.memorize([v, v2], { pool_nickname: "origin2" }).wait(10);
    const base = v.global_id.split("@")[0];
    assert.equal(laila.remember(`${base}@evolution=-1`, { pool_nickname: "cache2", persist: false }).data, 2);
    origin.close();
  });

  tsq("test_custom_scoped_entry_cannot_roundtrip_serialized_pool", () => {
    // Documents: entries with a non-ENTRY leading scope are not rebuildable.
    const c = Entry.contingent({ scopes: ["CUSTOM"], data: 5, state: EntryState.READY });
    laila.memorize(c, { pool_nickname: "sq" }).wait(10);
    assert.throws(() => laila.remember(c.global_id, { pool_nickname: "sq", persist: false }).wait(10), E.ValueError);
  });

  tsq("test_int_keyed_dict_roundtrips", (sq) => {
    // CD-1 end-to-end: non-str keys survive a pool round trip.
    const e = Entry.constant(new Map([[1, "a"], [2.5, "b"]]));
    laila.memorize(e, { pool_nickname: "sq" }).wait(10);
    assert.ok(sq.keys().includes(e.global_id));
    const out = laila.remember(e.global_id, { pool_nickname: "sq", persist: false }).wait(10);
    assert.ok(T.eq(out.data, new Map([[1, "a"], [2.5, "b"]])), `got ${T.str(out.data)}`);
  });

  tsq("test_concurrent_memorize_many_threads", (sq) => {
    const entries = range(60).map((i) => Entry.constant(i));
    const worker = (chunk) => {
      laila.memorize(chunk, { pool_nickname: "sq" }).wait(30);
    };
    const threads = range(6).map((i) => new TH.Thread({ target: worker, args: [entries.filter((_, k) => k % 6 === i)] }));
    for (const th of threads) th.start();
    for (const th of threads) th.join();
    assert.deepEqual(
      sorted(sq.keys()),
      sorted(entries.map((e) => e.global_id)),
    );
  });
});

// ---------------------------------------------------------------------------
// Central memory internals (unit)
// ---------------------------------------------------------------------------

describe("TestCentralMemoryUnit", () => {
  tp("test_scope", (fresh_policy) => {
    assert.deepEqual([...fresh_policy.central.memory.scopes], ["CENTRAL_MEMORY"]);
  });

  tp("test_alpha_pool_registered", (fresh_policy) => {
    const mem = fresh_policy.central.memory;
    assert.ok(dict_has(mem.pool_router.pools, mem.alpha_pool));
  });

  tp("test_resolve_pool_ref", (fresh_policy) => {
    const mem = fresh_policy.central.memory;
    const p = new DefaultPool();
    mem.extend(p, { pool_nickname: "rp" });
    assert.equal(mem._resolve_pool_ref(p), p);
    assert.equal(mem._resolve_pool_ref("rp"), p);
    assert.equal(mem._resolve_pool_ref(p.global_id), p);
    assert.throws(() => mem._resolve_pool_ref("nope"), E.KeyError);
    assert.throws(() => mem._resolve_pool_ref(5), E.TypeError);
  });

  tp("test_borrow_not_implemented", (fresh_policy) => {
    assert.throws(() => with_(fresh_policy.central.memory.borrow(), () => {}), E.NotImplementedError);
  });

  tp("test_fetch_borrow_not_implemented", (fresh_policy) => {
    assert.throws(() => fresh_policy.central.memory._fetch(["x"], { pool: new DefaultPool(), borrow: true }), E.NotImplementedError);
  });

  tp("test_pool_chain", () => {
    const [a, b, c] = [new DefaultPool(), new DefaultPool(), new DefaultPool()];
    a.__lshift__(b);
    b.__lshift__(c);
    const chain = _LAILA_IDENTIFIABLE_CENTRAL_MEMORY._pool_chain(a);
    assert.deepEqual(chain, [a, b, c]);
  });

  tp("test_pool_chain_cycle_terminates", () => {
    const [a, b] = [new DefaultPool(), new DefaultPool()];
    a.__lshift__(b);
    b.__lshift__(a);
    const chain = _LAILA_IDENTIFIABLE_CENTRAL_MEMORY._pool_chain(a);
    assert.ok(chain.length <= 2);
  });

  tp("test_key_evolution_static", () => {
    assert.equal(_LAILA_IDENTIFIABLE_CENTRAL_MEMORY._key_evolution(`LAILA:ENTRY:${U1}@evolution=4`), 4);
  });

  tp("test_read_entries_direct", (fresh_policy) => {
    const es = range(3).map((i) => Entry.constant(i));
    laila.memorize(es).wait(10);
    const ref = fresh_policy.central.memory._read_entries_direct(es.map((e) => e.global_id));
    const out = ref.wait(10);
    const data = (out !== null && typeof out === "object" && "data" in out ? out.data : out).map((x) => x.data);
    assert.deepEqual(data, [0, 1, 2]);
  });

  tp("test_read_entries_async", async (fresh_policy) => {
    const es = range(3).map((i) => Entry.constant(i));
    laila.memorize(es).wait(10);
    const out = await fresh_policy.central.memory._read_entries_async(es.map((e) => e.global_id));
    assert.deepEqual(
      out.map((x) => x.data),
      [0, 1, 2],
    );
  });

  tp("test_resolve_entry_key_async", async (fresh_policy) => {
    const v = Entry.variable(1);
    const v2 = v.evolve(2);
    laila.memorize([v, v2]).wait(10);
    const base = v.global_id.split("@")[0];
    const mem = fresh_policy.central.memory;
    const [key, raw] = await mem._resolve_entry_key_async(laila.alpha_pool, `${base}@evolution=-1`);
    assert.equal(key, v2.global_id);
    // raw is only pre-read for timestamp queries
    assert.equal(raw, null);
    const [key_exact] = await mem._resolve_entry_key_async(laila.alpha_pool, v.global_id);
    assert.equal(key_exact, v.global_id);
    const [key_ts] = await mem._resolve_entry_key_async(laila.alpha_pool, `${base}@creation_timestamp=${v2.creation_timestamp}`);
    assert.equal(key_ts, v2.global_id);
    // when the index answers, raw is not pre-read; only the scan fallback pre-reads
    laila.alpha_pool.index_enabled = false;
    const [key_ts2, raw_ts2] = await mem._resolve_entry_key_async(laila.alpha_pool, `${base}@creation_timestamp=${v2.creation_timestamp}`);
    assert.equal(key_ts2, v2.global_id);
    assert.notEqual(raw_ts2, null);
  });

  tp("test_resolve_entry_key_async_missing", async (fresh_policy) => {
    await assert.rejects(fresh_policy.central.memory._resolve_entry_key_async(laila.alpha_pool, `LAILA:ENTRY:${U1}`), E.KeyError);
  });
});

// ---------------------------------------------------------------------------
// Multi-policy (local) routing
// ---------------------------------------------------------------------------

describe("TestMultiPolicyLocal", () => {
  tp("test_two_local_policies_are_isolated", (fresh_policy) => {
    const other = new _LAILA_IDENTIFIABLE_POLICY();
    try {
      laila.activate_policy(other);
      laila.activate_policy(fresh_policy);
      const e = Entry.constant(1);
      laila.memorize(e, { policy_id: other.global_id }).wait(10);
      assert.throws(() => laila.remember(e.global_id).wait(10), E.KeyError);
      assert.equal(laila.remember(e.global_id, { policy_id: other.global_id }).data, 1);
      assert.equal(laila.get_active_policy(), fresh_policy);
    } finally {
      other.central.command.shutdown({ wait: true, cancel_pending: true });
    }
  });

  tp("test_policy_id_shorthand", (fresh_policy) => {
    const other = new _LAILA_IDENTIFIABLE_POLICY({ nickname: "other-pol" });
    try {
      laila.activate_policy(other);
      laila.activate_policy(fresh_policy);
      const e = Entry.constant(2);
      laila.memorize(e, { policy_id: "POLICY:other-pol" }).wait(10);
      assert.equal(laila.remember(e.global_id, { policy_id: "POLICY:other-pol" }).data, 2);
    } finally {
      other.central.command.shutdown({ wait: true, cancel_pending: true });
    }
  });

  tp("test_unknown_policy_id", () => {
    assert.throws(() => laila.memorize(Entry.constant(1), { policy_id: "LAILA:POLICY:" + U1 }), E.ConnectionError);
  });

  tp("test_local_policies_registry", (fresh_policy) => {
    assert.ok(dict_has(laila.local_policies, fresh_policy.global_id));
    assert.equal(laila.get_active_policy(), fresh_policy);
  });

  tp("test_active_policy_assignment", (fresh_policy) => {
    const other = new _LAILA_IDENTIFIABLE_POLICY();
    try {
      laila.active_policy = other;
      assert.equal(laila.get_active_policy(), other);
    } finally {
      laila.activate_policy(fresh_policy);
      other.central.command.shutdown({ wait: true, cancel_pending: true });
    }
  });
});

// ---------------------------------------------------------------------------
// Policy convenience API
// ---------------------------------------------------------------------------

describe("TestPolicyConvenienceAPI", () => {
  tp("test_scope_and_central", (fresh_policy) => {
    assert.deepEqual([...fresh_policy.scopes], ["POLICY"]);
    assert.notEqual(fresh_policy.central.memory, null);
    assert.notEqual(fresh_policy.central.command, null);
    assert.notEqual(fresh_policy.central.communication, null);
  });

  tp("test_policy_extend", (fresh_policy) => {
    const p = new DefaultPool();
    fresh_policy.extend(p);
    assert.ok(dict_has(fresh_policy.central.memory.pool_router.pools, p.global_id));
  });

  tp("test_policy_remember", (fresh_policy) => {
    const e = Entry.constant(1);
    laila.memorize(e).wait(10);
    const out = fresh_policy.remember(e.global_id);
    assert.notEqual(out, null);
  });

  tp("test_policy_memorize", (fresh_policy) => {
    const e = Entry.constant(1);
    fresh_policy.memorize([e]);
    assert.equal(laila.remember(e.global_id).data, 1);
  });

  tp("test_policy_remember_global_fetch_not_implemented", (fresh_policy) => {
    assert.throws(() => fresh_policy.remember("x", { global_fetch: true }), E.NotImplementedError);
  });

  tp("test_policy_future_bank", (fresh_policy) => {
    const f = laila.command.submit([() => 1]);
    assert.ok(dict_has(fresh_policy.future_bank, f.global_id));
    f.wait(10);
  });
});

// ---------------------------------------------------------------------------
// Logger hooks around memory ops (smoke)
// ---------------------------------------------------------------------------

describe("TestLoggerHooks", () => {
  tp("test_memorize_remember_forget_do_not_break_logger", () => {
    const lg = get_logger();
    const e = Entry.constant(1);
    laila.memorize(e).wait(10);
    laila.remember(e.global_id).wait(10);
    laila.forget(e.global_id).wait(10);
    assert.notEqual(lg, null);
  });
});

// Tear down the lazily-activated default policy so the process exits.
test("zz teardown", () =>
  macrotask(() => {
    laila.terminate();
    fs.rmSync(TMP_ROOT, { recursive: true, force: true });
  }));
