/**
 * Port of ``tests/deep_eval/test_06_pools_deep.py``.
 *
 * Deep tests for pool back-ends: DefaultPool (in-memory), SQLitePool,
 * DuckDBPool, proxy chains, PoolRouter, PoolWrapper and MultiBuffer.
 */
import { test, describe, beforeEach, afterEach } from "node:test";
import assert from "node:assert/strict";
import crypto from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { DatabaseSync } from "node:sqlite";

import { S, laila, macrotask, with_fresh_policy_async as with_fresh_policy } from "./_fixtures.js";

const E = await import(S + "_compat/errors.js");
const T = await import(S + "_compat/pytypes.js");
const { with_ } = await import(S + "_compat/contextlib.js");
const { ThreadPoolExecutor } = await import(S + "_compat/executor.js");
const defaults = await import(S + "macros/defaults.js");
const { DefaultPool, LAILA_DEFAULT_DIRECTORIES } = defaults;
const { _DEFAULT_POOL_NICKNAME } = await import(S + "macros/strings.js");
const { DuckDBPool } = await import(S + "data/duckdb/duckdb.js");
const { MultiBuffer } = await import(S + "data/multibuffer/multibuffer.js");
const { _LAILA_IDENTIFIABLE_POOL } = await import(S + "data/schema/base.js");
const { is_index_key } = await import(S + "data/schema/pool_index.js");
const { SQLitePool } = await import(S + "data/sqlite/sqlite.js");
const { PoolWrapper } = await import(S + "data/schema/pool_wrapper.js");
const { TransformationSequence } = await import(S + "entry/compdata/transformation/base.js");
const { Entry } = await import(S + "entry/entry.js");
const { Record } = await import(S + "policy/central/memory/record/record.js");
const { _LAILA_IDENTIFIABLE_POOL_ROUTER } = await import(S + "policy/central/memory/router/pool_router.js");
const { Manifest } = await import(S + "policy/central/memory/schema/manifest.js");

const { NotImplemented, dict_has, dict_len, dict_values, getitem, range } = T;

const U1 = "11111111-2222-3333-4444-555555555555";
const BASE = `LAILA:ENTRY:${U1}`;

const TMP_ROOT = fs.mkdtempSync(path.join(os.tmpdir(), "laila_deep06_"));
laila.set_default_directory(TMP_ROOT);

const t = (name, fn) => test(name, () => macrotask(fn));
const tp = (name, fn) => test(name, () => with_fresh_policy(fn));
const sorted = (xs) => [...xs].sort();
const tmp_path = () => fs.mkdtempSync(path.join(TMP_ROOT, "tmp_"));
const hex4 = () => crypto.randomBytes(4).toString("hex");
/** Python ``RecursionError`` <-> V8 "Maximum call stack size exceeded". */
const is_recursion_error = (e) => e instanceof E.RecursionError || (e instanceof RangeError && /call stack/i.test(e.message));

const _make_pools = (tmp) => ({
  default: () => new DefaultPool(),
  sqlite: () => new SQLitePool({ file_path: path.join(tmp, `${hex4()}.sqlite`) }),
  duckdb: () => new DuckDBPool({ file_path: path.join(tmp, `${hex4()}.duckdb`) }),
});

// ---------------------------------------------------------------------------
// Generic back-end contract (parametrized over back-ends)
// ---------------------------------------------------------------------------

for (const backend of ["default", "sqlite", "duckdb"]) {
  describe(`TestBackendContract[${backend}]`, () => {
    let pool;
    beforeEach(() =>
      macrotask(() => {
        pool = _make_pools(tmp_path())[backend]();
      }),
    );
    afterEach(() =>
      macrotask(() => {
        if (typeof pool?.close === "function") {
          try {
            pool.close();
          } catch {
            /* ignore */
          }
        }
      }),
    );

    t("test_identity_scope", () => {
      assert.deepEqual([...pool.scopes], ["POOL"]);
      assert.equal(pool.pool_id, pool.global_id);
      assert.ok(pool.global_id.startsWith("LAILA:POOL:"));
    });

    t("test_write_read", () => {
      pool.write("k", { a: 1 });
      assert.deepEqual(pool["k"], { a: 1 });
    });

    t("test_setitem_getitem", () => {
      pool["k"] = { a: 1 };
      assert.deepEqual(pool["k"], { a: 1 });
    });

    t("test_missing_returns_none", () => {
      assert.equal(pool["missing"], null);
    });

    t("test_exists_contains", () => {
      pool["k"] = { a: 1 };
      assert.ok(pool.exists("k"));
      assert.ok("k" in pool);
      assert.ok(!("z" in pool));
    });

    t("test_delete", () => {
      pool["k"] = { a: 1 };
      delete pool["k"];
      assert.equal(pool["k"], null);
      assert.ok(!("k" in pool));
    });

    t("test_delete_missing_is_noop", () => {
      pool.delete("nothing");
    });

    t("test_overwrite", () => {
      pool["k"] = { a: 1 };
      pool["k"] = { a: 2 };
      assert.deepEqual(pool["k"], { a: 2 });
    });

    t("test_keys_hide_index_keys", () => {
      pool[`${BASE}@evolution=0`] = { v: 1 };
      const keys = pool.keys();
      assert.ok(keys.includes(`${BASE}@evolution=0`));
      assert.ok(!keys.some((k) => is_index_key(k)));
      const with_index = [...pool.keys({ include_index: true })];
      assert.ok(with_index.length >= keys.length);
    });

    t("test_keys_generator", () => {
      pool["a"] = { v: 1 };
      pool["b"] = { v: 2 };
      const gen = pool.keys({ as_generator: true });
      assert.equal(typeof gen.next, "function"); // hasattr(gen, "__next__")
      assert.deepEqual(new Set(gen), new Set(["a", "b"]));
    });

    t("test_empty", () => {
      pool["a"] = { v: 1 };
      pool[`${BASE}@evolution=0`] = { v: 1 };
      pool.empty();
      assert.deepEqual(pool.keys(), []);
      assert.equal(pool["a"], null);
      assert.deepEqual([...pool.keys({ include_index: true })], []);
    });

    t("test_many_keys", () => {
      for (const i of range(50)) pool[`k${i}`] = { i };
      assert.equal(pool.keys().length, 50);
      assert.deepEqual(pool["k49"], { i: 49 });
    });

    t("test_unicode_key_and_value", () => {
      pool["ключ ✓"] = { v: "значение ✓" };
      assert.deepEqual(pool["ключ ✓"], { v: "значение ✓" });
      assert.ok(pool.keys().includes("ключ ✓"));
    });

    t("test_sync_not_implemented_for_cacheless", () => {
      assert.throws(() => pool.sync(), E.NotImplementedError);
    });

    test(
      "test_async_api",
      () =>
        macrotask(async () => {
      await pool.write_async("k", { a: 1 });
      assert.deepEqual(await pool._read_async("k"), { a: 1 });
      assert.ok(await pool._exists_async("k"));
      await pool.delete_async("k");
      assert.equal(await pool._read_async("k"), null);
        }),
    );

    t("test_thread_safety", () => {
      const worker = (i) => {
        for (const j of range(40)) pool[`${i}-${j}`] = { v: j };
      };
      with_(new ThreadPoolExecutor({ max_workers: 6 }), (ex) => [...ex.map(worker, range(6))]);
      assert.equal(pool.keys().length, 240);
    });

    t("test_record_roundtrip_through_pool", () => {
      const e = Entry.constant({ x: [1, 2] }, { uuid: U1 });
      const rec = new Record({ entry: e });
      const blob = rec.serialize(pool.transformations ?? new TransformationSequence());
      pool[e.global_id] = blob;
      const raw = pool[e.global_id];
      const rebuilt = Record._build_sync(raw);
      assert.deepEqual(getitem(rebuilt, "entry").data, { x: [1, 2] });
      assert.equal(getitem(rebuilt, "entry").global_id, e.global_id);
    });

    t("test_default_transformations", () => {
      if (pool instanceof DefaultPool && Object.getPrototypeOf(pool).constructor === _LAILA_IDENTIFIABLE_POOL) {
        assert.equal(pool.transformations, null);
      } else {
        assert.notEqual(pool.transformations, null);
        assert.deepEqual(
          [...pool.transformations].map((tr) => tr.name),
          ["base64"],
        );
      }
    });

    t("test_index_enabled_default", () => {
      assert.equal(pool.index_enabled, true);
      assert.equal(pool.batch_accelerated, false);
    });
  });
}

// ---------------------------------------------------------------------------
// SQLite / DuckDB specifics
// ---------------------------------------------------------------------------

describe("TestSQLite", () => {
  t("test_persists_across_instances", () => {
    const p = path.join(tmp_path(), "p.sqlite");
    const a = new SQLitePool({ file_path: p, uuid: U1 });
    a["k"] = { v: 1 };
    a.close();
    const b = new SQLitePool({ file_path: p, uuid: U1 });
    assert.deepEqual(b["k"], { v: 1 });
    b.close();
  });

  t("test_close_then_use_raises", () => {
    const p = new SQLitePool({ file_path: path.join(tmp_path(), "c.sqlite") });
    p.close();
    assert.throws(() => {
      p["k"] = { v: 1 };
    }, E.RuntimeError);
  });

  t("test_close_idempotent", () => {
    const p = new SQLitePool({ file_path: path.join(tmp_path(), "c.sqlite") });
    p.close();
    p.close();
  });

  t("test_default_path_derived_from_uuid", () => {
    const tmp = tmp_path();
    const saved = LAILA_DEFAULT_DIRECTORIES.pools;
    LAILA_DEFAULT_DIRECTORIES.pools = tmp; // monkeypatch.setitem
    try {
      const p = new SQLitePool();
      assert.ok(p.file_path.startsWith(tmp));
      assert.ok(p.file_path.includes(p.uuid));
      p.close();
    } finally {
      LAILA_DEFAULT_DIRECTORIES.pools = saved;
    }
  });

  t("test_values_are_json_text", () => {
    const p_ = path.join(tmp_path(), "j.sqlite");
    const p = new SQLitePool({ file_path: p_ });
    p["k"] = { v: [1, 2] };
    const db = new DatabaseSync(p_);
    try {
      const row = db.prepare("SELECT value FROM laila_pool_entries WHERE key='k'").get();
      assert.deepEqual(JSON.parse(row.value), { v: [1, 2] });
    } finally {
      db.close();
    }
    p.close();
  });

  t("test_non_json_value_rejected", () => {
    const p = new SQLitePool({ file_path: path.join(tmp_path(), "n.sqlite") });
    assert.throws(() => {
      p["k"] = { v: Buffer.from("bytes") };
    }, E.TypeError);
    p.close();
  });

  t("test_tilde_expanded", () => {
    const tmp = tmp_path();
    const saved = process.env.HOME;
    process.env.HOME = tmp; // monkeypatch.setenv("HOME", ...)
    try {
      const p = new SQLitePool({ file_path: "~/sub/x.sqlite" });
      assert.equal(p.file_path, path.join(tmp, "sub", "x.sqlite"));
      assert.ok(fs.existsSync(p.file_path));
      p.close();
    } finally {
      process.env.HOME = saved;
    }
  });
});

describe("TestDuckDB", () => {
  t("test_persists_across_instances", () => {
    const p = path.join(tmp_path(), "p.duckdb");
    const a = new DuckDBPool({ file_path: p, uuid: U1 });
    a["k"] = { v: 1 };
    a.close();
    const b = new DuckDBPool({ file_path: p, uuid: U1 });
    assert.deepEqual(b["k"], { v: 1 });
    b.close();
  });

  t("test_close_then_use_raises", () => {
    const p = new DuckDBPool({ file_path: path.join(tmp_path(), "c.duckdb") });
    p.close();
    assert.throws(() => {
      p["k"] = { v: 1 };
    });
  });

  t("test_many_entries_and_enumeration_order_independent", () => {
    const p = new DuckDBPool({ file_path: path.join(tmp_path(), "m.duckdb") });
    for (const i of range(30)) p[`k${String(i).padStart(2, "0")}`] = { i };
    assert.deepEqual(
      sorted(p.keys()),
      range(30).map((i) => `k${String(i).padStart(2, "0")}`),
    );
    p.close();
  });
});

// ---------------------------------------------------------------------------
// Proxy chains
// ---------------------------------------------------------------------------

describe("TestProxy", () => {
  t("test_proxy_property_read_is_none", () => {
    assert.equal(new DefaultPool().proxy, null);
  });

  t("test_proxy_setter_sets_proxy_to_on_cache", () => {
    const [origin, cache] = [new DefaultPool(), new DefaultPool()];
    origin.proxy = cache;
    assert.equal(cache.proxy_to, origin);
    assert.equal(origin.proxy_to, null);
  });

  t("test_lshift", () => {
    const [origin, cache] = [new DefaultPool(), new DefaultPool()];
    const result = cache.__lshift__(origin);
    assert.equal(cache.proxy_to, origin);
    assert.ok(result === cache || result === origin || result === null);
  });

  t("test_rshift", () => {
    const [origin, cache] = [new DefaultPool(), new DefaultPool()];
    origin.__rshift__(cache);
    assert.equal(cache.proxy_to, origin);
  });

  t("test_read_through_and_write_back", () => {
    const [origin, cache] = [new DefaultPool(), new DefaultPool()];
    cache.__lshift__(origin);
    origin["k"] = { v: 1 };
    assert.ok(!("k" in cache));
    assert.deepEqual(cache["k"], { v: 1 });
    assert.ok("k" in cache); // cached
  });

  t("test_cache_write_does_not_propagate", () => {
    const [origin, cache] = [new DefaultPool(), new DefaultPool()];
    cache.__lshift__(origin);
    cache["k"] = { v: 1 };
    assert.equal(origin["k"], null);
  });

  t("test_cache_delete_does_not_propagate", () => {
    const [origin, cache] = [new DefaultPool(), new DefaultPool()];
    cache.__lshift__(origin);
    origin["k"] = { v: 1 };
    assert.deepEqual(cache["k"], { v: 1 });
    delete cache["k"];
    assert.deepEqual(origin["k"], { v: 1 });
    assert.deepEqual(cache["k"], { v: 1 }); // re-fetched
  });

  t("test_three_tier_chain", () => {
    const [a, b, c] = [new DefaultPool(), new DefaultPool(), new DefaultPool()];
    b.__lshift__(c);
    a.__lshift__(b);
    c["k"] = { v: 1 };
    assert.deepEqual(a["k"], { v: 1 });
    assert.ok("k" in b);
    assert.ok("k" in a);
  });

  t("test_miss_in_whole_chain", () => {
    const [a, b] = [new DefaultPool(), new DefaultPool()];
    a.__lshift__(b);
    assert.equal(a["nothing"], null);
  });

  t("test_exists_is_local_only", () => {
    const [origin, cache] = [new DefaultPool(), new DefaultPool()];
    cache.__lshift__(origin);
    origin["k"] = { v: 1 };
    assert.equal(cache.exists("k"), false);
  });

  t("test_keys_local_only", () => {
    const [origin, cache] = [new DefaultPool(), new DefaultPool()];
    cache.__lshift__(origin);
    origin["k"] = { v: 1 };
    assert.deepEqual(cache.keys(), []);
  });

  t("test_read_through_async", async () => {
    const [origin, cache] = [new DefaultPool(), new DefaultPool()];
    cache.__lshift__(origin);
    origin["k"] = { v: 1 };
    assert.deepEqual(await cache._read_through_async("k"), { v: 1 });
    assert.deepEqual(await cache._read_async("k"), { v: 1 });
  });

  t("test_proxy_with_sqlite_origin", () => {
    const origin = new SQLitePool({ file_path: path.join(tmp_path(), "o.sqlite") });
    const cache = new DefaultPool();
    cache.__lshift__(origin);
    origin["k"] = { v: 1 };
    assert.deepEqual(cache["k"], { v: 1 });
    origin.close();
  });

  t("test_self_proxy_cycle_guard", () => {
    // Documents: a pool can be made its own proxy; a miss then recurses forever.
    const p = new DefaultPool();
    p.__lshift__(p);
    assert.equal(p.proxy_to, p);
    assert.throws(() => p["missing"], is_recursion_error);
  });

  t("test_proxy_cycle_two_pools", () => {
    const [a, b] = [new DefaultPool(), new DefaultPool()];
    a.__lshift__(b);
    b.__lshift__(a);
    assert.throws(() => a["missing"], is_recursion_error);
  });
});

// ---------------------------------------------------------------------------
// Bulk copy (<=)
// ---------------------------------------------------------------------------

describe("TestBulkCopy", () => {
  tp("test_le_copies_entries", (fresh_policy) => {
    const [src, dst] = [new DefaultPool(), new DefaultPool()];
    fresh_policy.central.memory.extend(src, { pool_nickname: "src" });
    fresh_policy.central.memory.extend(dst, { pool_nickname: "dst" });
    const [e1, e2] = [Entry.constant(1), Entry.variable(2)];
    laila.memorize([e1, e2], { pool_nickname: "src" }).wait(10);
    const ref = dst.__le__(src);
    if (ref !== null && ref !== undefined && typeof ref.wait === "function") ref.wait(10);
    const keys = new Set(dst.keys());
    assert.ok(keys.has(e1.global_id) && keys.has(e2.global_id));
  });

  tp("test_le_by_nickname", (fresh_policy) => {
    const [src, dst] = [new DefaultPool(), new DefaultPool()];
    fresh_policy.central.memory.extend(src, { pool_nickname: "src2" });
    fresh_policy.central.memory.extend(dst, { pool_nickname: "dst2" });
    const e1 = Entry.constant(1);
    laila.memorize(e1, { pool_nickname: "src2" }).wait(10);
    const ref = dst.__le__("src2");
    if (ref !== null && ref !== undefined && typeof ref.wait === "function") ref.wait(10);
    assert.ok(dst.keys().includes(e1.global_id));
  });

  tp("test_le_unregistered_destination_is_a_clear_error", (fresh_policy) => {
    const [src, dst] = [new DefaultPool(), new DefaultPool()];
    fresh_policy.central.memory.extend(src, { pool_nickname: "src3" });
    const e1 = Entry.constant(1);
    laila.memorize(e1, { pool_nickname: "src3" }).wait(10);
    assert.throws(
      () => dst.__le__(src),
      (e) => e instanceof E.KeyError && /not registered with central memory/.test(e.message),
    );
    fresh_policy.central.memory.extend(dst, { pool_nickname: "dst3" });
    dst.__le__(src).wait(10);
    assert.ok(dst.keys().includes(e1.global_id));
  });

  t("test_le_invalid_type", () => {
    // ``DefaultPool() <= 5`` raises TypeError in Python because ``__le__``
    // answers ``NotImplemented``; JS has no operator dispatch, so the
    // observable contract is that ``NotImplemented`` sentinel.
    assert.equal(new DefaultPool().__le__(5), NotImplemented);
  });
});

// ---------------------------------------------------------------------------
// PoolRouter
// ---------------------------------------------------------------------------

describe("TestPoolRouter", () => {
  t("test_default_pool_registered", () => {
    const r = new _LAILA_IDENTIFIABLE_POOL_ROUTER();
    assert.equal(dict_len(r.pools), 1);
    assert.ok(dict_has(r.pools_nicknames, _DEFAULT_POOL_NICKNAME));
    assert.deepEqual([...r.scopes], ["POOL_ROUTER"]);
  });

  t("test_route_default", () => {
    const r = new _LAILA_IDENTIFIABLE_POOL_ROUTER();
    const p = r.route([]);
    assert.equal(p.global_id, getitem(r.pools_nicknames, _DEFAULT_POOL_NICKNAME));
  });

  t("test_extend_and_route_by_nickname", () => {
    const r = new _LAILA_IDENTIFIABLE_POOL_ROUTER();
    const p = new DefaultPool();
    r.extend(p, { pool_nickname: "x", affinity: 0.5 });
    assert.equal(r.route([], { pool_nickname: "x" }), p);
    assert.equal(r.route([], { pool_id: p.global_id }), p);
  });

  t("test_route_unknown_nickname", () => {
    const r = new _LAILA_IDENTIFIABLE_POOL_ROUTER();
    assert.throws(() => r.route([], { pool_nickname: "nope" }), E.KeyError);
  });

  t("test_route_unknown_pool_id", () => {
    const r = new _LAILA_IDENTIFIABLE_POOL_ROUTER();
    assert.throws(() => r.route([], { pool_id: "LAILA:POOL:" + U1 }), E.KeyError);
  });

  t("test_pool_id_takes_precedence_over_nickname", () => {
    const r = new _LAILA_IDENTIFIABLE_POOL_ROUTER();
    const [p1, p2] = [new DefaultPool(), new DefaultPool()];
    r.extend(p1, { pool_nickname: "a" });
    r.extend(p2, { pool_nickname: "b" });
    assert.equal(r.route([], { pool_id: p1.global_id, pool_nickname: "b" }), p1);
  });

  t("test_extend_without_nickname", () => {
    const r = new _LAILA_IDENTIFIABLE_POOL_ROUTER();
    const p = new DefaultPool();
    r.extend(p);
    assert.ok(dict_has(r.pools, p.global_id));
    assert.ok(!dict_values(r.pools_nicknames).includes(p.global_id));
  });

  t("test_nickname_silently_rebound", () => {
    // Documents: re-using a nickname silently rebinds to the new pool.
    const r = new _LAILA_IDENTIFIABLE_POOL_ROUTER();
    const [p1, p2] = [new DefaultPool(), new DefaultPool()];
    r.extend(p1, { pool_nickname: "x" });
    r.extend(p2, { pool_nickname: "x" });
    assert.equal(r.route([], { pool_nickname: "x" }), p2);
    assert.ok(dict_has(r.pools, p1.global_id));
  });

  t("test_default_nickname_can_be_hijacked", () => {
    // Documents: nothing protects the alpha nickname from being rebound.
    const r = new _LAILA_IDENTIFIABLE_POOL_ROUTER();
    const p = new DefaultPool();
    r.extend(p, { pool_nickname: _DEFAULT_POOL_NICKNAME });
    assert.equal(r.route([]), p);
  });
});

// ---------------------------------------------------------------------------
// PoolWrapper via manifest indexing
// ---------------------------------------------------------------------------

describe("TestPoolWrapper", () => {
  t("test_pool_indexed_by_manifest_returns_wrapper", () => {
    const p = new DefaultPool();
    const m = new Manifest({ data: { a: BASE } });
    const w = p.__getitem__(m);
    assert.ok(w instanceof PoolWrapper);
  });
});

// ---------------------------------------------------------------------------
// MultiBuffer
// ---------------------------------------------------------------------------

describe("TestMultiBuffer", () => {
  t("test_defaults", () => {
    const mb = new MultiBuffer();
    assert.equal(mb.capacity, 2);
    assert.equal(mb.__len__(), 2);
    assert.ok(mb.read_head === 0 && mb.write_head === 0);
    assert.deepEqual([...mb.scopes], ["MULTI_BUFFER"]);
  });

  t("test_capacity_ge_1", () => {
    assert.throws(() => new MultiBuffer({ capacity: 0 }));
  });

  t("test_slots_override_sets_capacity", () => {
    const mb = new MultiBuffer({ slots: new Array(5).fill(null) });
    assert.equal(mb.capacity, 5);
  });

  t("test_slots_empty_rejected", () => {
    assert.throws(() => new MultiBuffer({ slots: [] }), E.ValueError);
  });

  t("test_slots_unsized_rejected", () => {
    assert.throws(() => new MultiBuffer({ slots: 5 }), E.TypeError);
  });

  t("test_write_read_fifo", () => {
    const mb = new MultiBuffer({ capacity: 3 });
    assert.equal(mb.write("a"), 0);
    assert.equal(mb.write("b"), 1);
    assert.equal(mb.read().data, "a");
    assert.equal(mb.read().data, "b");
    assert.equal(mb.read(), null);
  });

  t("test_wraparound", () => {
    const mb = new MultiBuffer({ capacity: 2 });
    mb.write(1);
    mb.write(2);
    assert.equal(mb.write(3), 0); // overwrites slot 0
    assert.equal(mb.write_head, 1);
    assert.equal(mb[0].data, 3);
  });

  t("test_getitem_wraps_raw_in_entry", () => {
    const mb = new MultiBuffer();
    mb[0] = { x: 1 };
    const e = mb[0];
    assert.ok(e instanceof Entry);
    assert.deepEqual(e.data, { x: 1 });
  });

  t("test_setitem_entry_and_record", () => {
    const mb = new MultiBuffer();
    const e = Entry.constant(5);
    mb[0] = e;
    assert.equal(mb[0], e);
    mb[1] = new Record({ entry: e });
    assert.equal(mb[1], e);
  });

  t("test_setitem_none_clears", () => {
    const mb = new MultiBuffer();
    mb[0] = 1;
    mb[0] = null;
    assert.equal(mb[0], null);
  });

  t("test_index_modulo", () => {
    const mb = new MultiBuffer({ capacity: 3 });
    mb[4] = "x";
    assert.equal(mb[1].data, "x");
    assert.equal(mb[-2].data, "x");
  });

  t("test_index_type_checked", () => {
    const mb = new MultiBuffer();
    assert.throws(() => mb["a"], E.TypeError);
    assert.throws(() => mb.__getitem__(true), E.TypeError);
  });

  t("test_empty", () => {
    const mb = new MultiBuffer({ capacity: 2 });
    mb.write(1);
    mb.read();
    mb.empty();
    assert.ok(mb.read_head === 0 && mb.write_head === 0);
    assert.ok(mb[0] === null && mb[1] === null);
  });

  t("test_unmapped_write_requires_value", () => {
    assert.throws(() => new MultiBuffer().write(), E.TypeError);
  });

  t("test_mapped_write_without_value_moves_head", () => {
    const mb = new MultiBuffer({ mapped: true });
    assert.equal(mb.write(), 0);
    assert.equal(mb.write_head, 1);
  });

  t("test_mapped_write_raw", () => {
    const mb = new MultiBuffer({ mapped: true });
    const raw = Buffer.from("raw");
    mb.write(raw);
    assert.equal(mb.slots[0], raw);
    assert.deepEqual(Buffer.from(mb.read().data), Buffer.from("raw"));
  });

  t("test_thread_safe_writes", () => {
    const mb = new MultiBuffer({ capacity: 1000 });
    const worker = (i) => {
      for (const j of range(50)) mb.write(T.tuple([i, j]));
    };
    with_(new ThreadPoolExecutor({ max_workers: 8 }), (ex) => [...ex.map(worker, range(8))]);
    assert.equal(mb.write_head, 400);
    assert.equal(mb.slots.filter((s) => s !== null).length, 400);
  });
});

// Tear down the lazily-activated default policy so the process exits.
test("zz teardown", () =>
  macrotask(() => {
    laila.terminate();
    fs.rmSync(TMP_ROOT, { recursive: true, force: true });
  }));
