/**
 * Data sub-package (schema level): ports of
 *   tests/functional/pools/base/unit_tests/test_pool_base.py
 *   tests/functional/pools/base/unit_tests/test_pool_index.py
 *   tests/functional/pools/data_container/unit_tests/test_data_container.py
 *   tests/functional/pools/multibuffer/unit_tests/test_multibuffer.py
 *   tests/functional/pools/mempool/unit_tests/test_memory_pool.py
 *   tests/functional/pools/mempool/unit_tests/test_memory_compdata_roundtrip.py
 *
 * Concrete backends (SQLite / DuckDB / HDF5 / Filesystem) live in
 * ``pools_local.test.js``; the networked backends are exercised by the
 * functional tree against their mock servers.
 */
import { test, describe, before, after, beforeEach, afterEach } from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

const { default: laila, S } = await import("./fixtures/laila_root.js");
const RT = await import("./fixtures/compdata_roundtrip.js");

const E = await import(S + "_compat/errors.js");
const T = await import(S + "_compat/pytypes.js");
const TH = await import(S + "_compat/threading.js");
const time = await import(S + "_compat/time.js");
const { with_ } = await import(S + "_compat/contextlib.js");
const { ValidationError } = await import(S + "_compat/pydantic.js");
const { _LAILA_IDENTIFIABLE_POOL } = await import(S + "data/schema/base.js");
const { _LAILA_IDENTIFIABLE_DATA_CONTAINER } = await import(S + "data/schema/data_container.js");
const { PoolIndex, is_index_key } = await import(S + "data/schema/pool_index.js");
const { MultiBuffer } = await import(S + "data/multibuffer/multibuffer.js");
const { SQLitePool } = await import(S + "data/sqlite/sqlite.js");
const { _LAILA_IDENTIFIABLE_POLICY } = await import(S + "policy/schema/base.js");
const { Record } = await import(S + "policy/central/memory/record/record.js");
const { Entry } = await import(S + "entry/index.js");
const { _eligible_model_fields } = await import(S + "basics/definitions/cli_capable.js");
const { _DATA_CONTAINER_SCOPE, _MULTI_BUFFER_SCOPE, _POOL_SCOPE } = await import(S + "macros/strings.js");

const { NotImplemented, PyByteArray } = T;
const B = (s) => Buffer.from(s, "latin1");

// Keep default-directory pools (``SQLitePool()`` without ``file_path``) out
// of ``~/.laila`` for the duration of this file.
const TMP_ROOT = fs.mkdtempSync(path.join(os.tmpdir(), "laila_data_test_"));
laila.set_default_directory(TMP_ROOT);

/**
 * Run a body on a fresh macrotask: ``node:test`` invokes test bodies from a
 * microtask, where a blocking wait (``Future.wait`` / ``Thread.join``) is
 * impossible by construction.
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
const t = (name, fn) => test(name, () => macrotask(fn));

const sorted = (xs) => [...xs].sort();
const count_equal = (a, b) => assert.deepEqual(sorted(a), sorted(b));
const _base = (entry) => entry.global_id.split("@")[0];

/** Fresh test policy per test (``setUp`` / ``tearDown`` of the Python suites). */
function use_test_policy(ctx) {
  beforeEach(() =>
    macrotask(() => {
      ctx.original = laila.get_active_policy();
      ctx.policy = new _LAILA_IDENTIFIABLE_POLICY();
      laila.activate_policy(ctx.policy);
      ctx.memory = ctx.policy.central.memory;
    }),
  );
  afterEach(() =>
    macrotask(() => {
      try {
        laila.get_active_policy().central.command.shutdown({ wait: true, cancel_pending: true });
      } catch {
        // ignore
      }
      laila.activate_policy(ctx.original);
    }),
  );
}

// ---------------------------------------------------------------------------
// test_pool_base.py
// ---------------------------------------------------------------------------
describe("TestPoolBaseGetItem", () => {
  test("get existing key", () => {
    const pool = new _LAILA_IDENTIFIABLE_POOL();
    pool["k1"] = "value";
    assert.equal(pool["k1"], "value");
  });
  test("get missing key returns none", () => {
    const pool = new _LAILA_IDENTIFIABLE_POOL();
    assert.equal(pool["nonexistent"], null);
  });
  test("get key storing none returns none", () => {
    const pool = new _LAILA_IDENTIFIABLE_POOL();
    pool["k"] = null;
    assert.equal(pool["k"], null);
  });
  test("missing and none indistinguishable", () => {
    const pool = new _LAILA_IDENTIFIABLE_POOL();
    pool["stored_none"] = null;
    assert.equal(pool["stored_none"], pool["never_set"]);
  });
  test("overwrite value", () => {
    const pool = new _LAILA_IDENTIFIABLE_POOL();
    pool["k"] = "first";
    pool["k"] = "second";
    assert.equal(pool["k"], "second");
  });
});

describe("TestPoolBaseSetItem", () => {
  const cases = [
    ["string", "hello"],
    ["dict", { nested: true }],
    ["list", [1, 2, 3]],
    ["int", 42],
    ["none", null],
    ["bytes", B("binary")],
  ];
  for (const [name, value] of cases) {
    test(`set ${name} value`, () => {
      const pool = new _LAILA_IDENTIFIABLE_POOL();
      pool["k"] = value;
      assert.deepEqual(pool["k"], value);
    });
  }
});

describe("TestPoolBaseDelItem", () => {
  test("delete existing key", () => {
    const pool = new _LAILA_IDENTIFIABLE_POOL();
    pool["k"] = "val";
    delete pool["k"];
    assert.equal(pool["k"], null);
  });
  test("delete missing key no error", () => {
    const pool = new _LAILA_IDENTIFIABLE_POOL();
    delete pool["nonexistent"];
  });
  test("delete twice no error", () => {
    const pool = new _LAILA_IDENTIFIABLE_POOL();
    pool["k"] = "val";
    delete pool["k"];
    delete pool["k"];
  });
});

describe("TestPoolBaseEmpty", () => {
  test("empty clears all", () => {
    const pool = new _LAILA_IDENTIFIABLE_POOL();
    pool["a"] = 1;
    pool["b"] = 2;
    pool.empty();
    assert.deepEqual([...pool.keys()], []);
  });
  test("empty on empty pool", () => {
    const pool = new _LAILA_IDENTIFIABLE_POOL();
    pool.empty();
    assert.deepEqual([...pool.keys()], []);
  });
});

describe("TestPoolBaseExists", () => {
  test("exists true", () => {
    const pool = new _LAILA_IDENTIFIABLE_POOL();
    pool["k"] = "val";
    assert.equal(pool.exists("k"), true);
  });
  test("exists false", () => {
    assert.equal(new _LAILA_IDENTIFIABLE_POOL().exists("missing"), false);
  });
  test("exists after delete", () => {
    const pool = new _LAILA_IDENTIFIABLE_POOL();
    pool["k"] = "val";
    delete pool["k"];
    assert.equal(pool.exists("k"), false);
  });
  test("contains operator", () => {
    const pool = new _LAILA_IDENTIFIABLE_POOL();
    pool["k"] = "val";
    assert.equal(pool.__contains__("k"), true);
    assert.equal("k" in pool, true);
    assert.equal(pool.__contains__("missing"), false);
    assert.equal("missing" in pool, false);
  });
  test("exists with none value", () => {
    const pool = new _LAILA_IDENTIFIABLE_POOL();
    pool["k"] = null;
    assert.equal(pool.exists("k"), true);
  });
});

describe("TestPoolBaseKeys", () => {
  test("keys returns list by default", () => {
    const pool = new _LAILA_IDENTIFIABLE_POOL();
    pool["a"] = 1;
    pool["b"] = 2;
    const keys = pool.keys();
    assert.ok(Array.isArray(keys));
    count_equal(keys, ["a", "b"]);
  });
  test("keys as generator", () => {
    const pool = new _LAILA_IDENTIFIABLE_POOL();
    pool["a"] = 1;
    const gen = pool.keys({ as_generator: true });
    assert.ok(!Array.isArray(gen));
    count_equal([...gen], ["a"]);
  });
  test("keys empty pool", () => {
    assert.deepEqual(new _LAILA_IDENTIFIABLE_POOL().keys(), []);
  });
  test("keys after empty", () => {
    const pool = new _LAILA_IDENTIFIABLE_POOL();
    pool["a"] = 1;
    pool.empty();
    assert.deepEqual(pool.keys(), []);
  });
});

describe("TestPoolBaseSync", () => {
  test("sync raises", () => {
    assert.throws(() => new _LAILA_IDENTIFIABLE_POOL().sync(), E.NotImplementedError);
  });
});

describe("TestPoolBasePoolId", () => {
  test("pool id matches global id", () => {
    const pool = new _LAILA_IDENTIFIABLE_POOL();
    assert.equal(pool.pool_id, pool.global_id);
  });
  test("different pools have different ids", () => {
    assert.notEqual(new _LAILA_IDENTIFIABLE_POOL().pool_id, new _LAILA_IDENTIFIABLE_POOL().pool_id);
  });
});

describe("TestPoolBaseLeOperator", () => {
  test("le with incompatible type returns NotImplemented", () => {
    assert.equal(new _LAILA_IDENTIFIABLE_POOL().__le__(42), NotImplemented);
  });
  test("le with none returns NotImplemented", () => {
    assert.equal(new _LAILA_IDENTIFIABLE_POOL().__le__(null), NotImplemented);
  });
});

describe("TestPoolBaseConcurrency", () => {
  test("atomic context manager", () => {
    const pool = new _LAILA_IDENTIFIABLE_POOL();
    with_(pool.atomic(), () => {
      pool.resource["k"] = "val";
    });
    assert.equal(pool["k"], "val");
  });
  test("multiple sequential writes", () => {
    const pool = new _LAILA_IDENTIFIABLE_POOL();
    for (let i = 0; i < 100; i++) pool[`key-${i}`] = i;
    assert.equal(pool.keys().length, 100);
    for (let i = 0; i < 100; i++) assert.equal(pool[`key-${i}`], i);
  });
});

describe("TestProxyProperties", () => {
  test("proxy_to default is none", () => {
    assert.equal(new _LAILA_IDENTIFIABLE_POOL().proxy_to, null);
  });
  test("proxy_to setter", () => {
    const cache = new _LAILA_IDENTIFIABLE_POOL();
    const origin = new _LAILA_IDENTIFIABLE_POOL();
    cache.proxy_to = origin;
    assert.equal(cache.proxy_to, origin);
  });
  test("proxy_to setter none clears", () => {
    const cache = new _LAILA_IDENTIFIABLE_POOL();
    cache.proxy_to = new _LAILA_IDENTIFIABLE_POOL();
    cache.proxy_to = null;
    assert.equal(cache.proxy_to, null);
  });
  test("proxy setter sets target proxy_to", () => {
    const cache = new _LAILA_IDENTIFIABLE_POOL();
    const origin = new _LAILA_IDENTIFIABLE_POOL();
    origin.proxy = cache;
    assert.equal(cache.proxy_to, origin);
  });
  test("proxy getter always returns none", () => {
    assert.equal(new _LAILA_IDENTIFIABLE_POOL().proxy, null);
  });
  test("proxy setter with none is noop", () => {
    const origin = new _LAILA_IDENTIFIABLE_POOL();
    origin.proxy = null;
  });
});

describe("TestProxyOperators", () => {
  test("lshift sets proxy_to", () => {
    const mem = new _LAILA_IDENTIFIABLE_POOL();
    const hdf5 = new _LAILA_IDENTIFIABLE_POOL();
    const result = mem.__lshift__(hdf5);
    assert.equal(mem.proxy_to, hdf5);
    assert.equal(result, hdf5);
  });
  test("rshift sets proxy_to", () => {
    const s3 = new _LAILA_IDENTIFIABLE_POOL();
    const hdf5 = new _LAILA_IDENTIFIABLE_POOL();
    const result = s3.__rshift__(hdf5);
    assert.equal(hdf5.proxy_to, s3);
    assert.equal(result, hdf5);
  });
  test("lshift chain three pools", () => {
    const mem = new _LAILA_IDENTIFIABLE_POOL();
    const hdf5 = new _LAILA_IDENTIFIABLE_POOL();
    const s3 = new _LAILA_IDENTIFIABLE_POOL();
    mem.__lshift__(hdf5).__lshift__(s3);
    assert.equal(mem.proxy_to, hdf5);
    assert.equal(hdf5.proxy_to, s3);
    assert.equal(s3.proxy_to, null);
  });
  test("rshift chain three pools", () => {
    const s3 = new _LAILA_IDENTIFIABLE_POOL();
    const hdf5 = new _LAILA_IDENTIFIABLE_POOL();
    const mem = new _LAILA_IDENTIFIABLE_POOL();
    s3.__rshift__(hdf5).__rshift__(mem);
    assert.equal(hdf5.proxy_to, s3);
    assert.equal(mem.proxy_to, hdf5);
    assert.equal(s3.proxy_to, null);
  });
});

describe("TestProxyRead", () => {
  test("read local hit no proxy", () => {
    const pool = new _LAILA_IDENTIFIABLE_POOL();
    pool["k"] = "local";
    assert.equal(pool["k"], "local");
  });
  test("read miss no proxy returns none", () => {
    assert.equal(new _LAILA_IDENTIFIABLE_POOL()["k"], null);
  });
  test("read miss falls back to proxy_to", () => {
    const cache = new _LAILA_IDENTIFIABLE_POOL();
    const origin = new _LAILA_IDENTIFIABLE_POOL();
    origin["k"] = "from_origin";
    cache.proxy_to = origin;
    assert.equal(cache["k"], "from_origin");
  });
  test("read fallback caches in local", () => {
    const cache = new _LAILA_IDENTIFIABLE_POOL();
    const origin = new _LAILA_IDENTIFIABLE_POOL();
    origin["k"] = "from_origin";
    cache.proxy_to = origin;
    void cache["k"];
    assert.equal(cache._exists("k"), true);
    assert.equal(cache._read("k"), "from_origin");
  });
  test("read local hit does not query origin", () => {
    const cache = new _LAILA_IDENTIFIABLE_POOL();
    const origin = new _LAILA_IDENTIFIABLE_POOL();
    origin["k"] = "origin_val";
    cache["k"] = "local_val";
    cache.proxy_to = origin;
    assert.equal(cache["k"], "local_val");
  });
  test("read miss on both returns none", () => {
    const cache = new _LAILA_IDENTIFIABLE_POOL();
    cache.proxy_to = new _LAILA_IDENTIFIABLE_POOL();
    assert.equal(cache["missing"], null);
  });
  test("read three level chain", () => {
    const mem = new _LAILA_IDENTIFIABLE_POOL();
    const hdf5 = new _LAILA_IDENTIFIABLE_POOL();
    const s3 = new _LAILA_IDENTIFIABLE_POOL();
    mem.__lshift__(hdf5).__lshift__(s3);
    s3["deep"] = "origin_value";
    assert.equal(mem["deep"], "origin_value");
    assert.equal(hdf5._exists("deep"), true);
    assert.equal(mem._exists("deep"), true);
  });
  test("read does not propagate upward", () => {
    const mem = new _LAILA_IDENTIFIABLE_POOL();
    const hdf5 = new _LAILA_IDENTIFIABLE_POOL();
    const s3 = new _LAILA_IDENTIFIABLE_POOL();
    mem.__lshift__(hdf5).__lshift__(s3);
    mem["only_in_mem"] = "mem_val";
    assert.equal(hdf5["only_in_mem"], null);
    assert.equal(s3["only_in_mem"], null);
  });
});

describe("TestProxyWrite", () => {
  test("write does not propagate to origin", () => {
    const cache = new _LAILA_IDENTIFIABLE_POOL();
    const origin = new _LAILA_IDENTIFIABLE_POOL();
    cache.proxy_to = origin;
    cache["k"] = "val";
    assert.equal(origin.exists("k"), false);
  });
  test("write does not propagate to proxy", () => {
    const cache = new _LAILA_IDENTIFIABLE_POOL();
    const origin = new _LAILA_IDENTIFIABLE_POOL();
    origin.proxy = cache;
    origin["k"] = "val";
    assert.equal(cache.exists("k"), false);
  });
  test("write in chain is local only", () => {
    const mem = new _LAILA_IDENTIFIABLE_POOL();
    const hdf5 = new _LAILA_IDENTIFIABLE_POOL();
    const s3 = new _LAILA_IDENTIFIABLE_POOL();
    mem.__lshift__(hdf5).__lshift__(s3);
    hdf5["new"] = "data";
    assert.equal(mem.exists("new"), false);
    assert.equal(s3.exists("new"), false);
    assert.equal(hdf5.exists("new"), true);
  });
});

describe("TestProxyDelete", () => {
  test("delete does not propagate to origin", () => {
    const cache = new _LAILA_IDENTIFIABLE_POOL();
    const origin = new _LAILA_IDENTIFIABLE_POOL();
    cache.proxy_to = origin;
    origin["k"] = "val";
    cache["k"] = "val";
    delete cache["k"];
    assert.equal(origin.exists("k"), true);
  });
  test("delete does not propagate to proxy", () => {
    const cache = new _LAILA_IDENTIFIABLE_POOL();
    const origin = new _LAILA_IDENTIFIABLE_POOL();
    origin.proxy = cache;
    cache["k"] = "val";
    origin["k"] = "val";
    delete origin["k"];
    assert.equal(cache.exists("k"), true);
  });
  test("delete in chain is local only", () => {
    const mem = new _LAILA_IDENTIFIABLE_POOL();
    const hdf5 = new _LAILA_IDENTIFIABLE_POOL();
    const s3 = new _LAILA_IDENTIFIABLE_POOL();
    mem.__lshift__(hdf5).__lshift__(s3);
    s3["k"] = "v";
    void mem["k"];
    delete hdf5["k"];
    assert.equal(mem.exists("k"), true);
    assert.equal(hdf5.exists("k"), false);
    assert.equal(s3.exists("k"), true);
  });
});

describe("TestProxyExists", () => {
  test("exists does not check origin", () => {
    const cache = new _LAILA_IDENTIFIABLE_POOL();
    const origin = new _LAILA_IDENTIFIABLE_POOL();
    cache.proxy_to = origin;
    origin["k"] = "val";
    assert.equal(cache.exists("k"), false);
  });
  test("exists after cache fill", () => {
    const cache = new _LAILA_IDENTIFIABLE_POOL();
    const origin = new _LAILA_IDENTIFIABLE_POOL();
    cache.proxy_to = origin;
    origin["k"] = "val";
    void cache["k"];
    assert.equal(cache.exists("k"), true);
  });
  test("contains is local only", () => {
    const cache = new _LAILA_IDENTIFIABLE_POOL();
    const origin = new _LAILA_IDENTIFIABLE_POOL();
    cache.proxy_to = origin;
    origin["k"] = "val";
    assert.equal("k" in cache, false);
  });
});

describe("TestProxyEmpty", () => {
  test("empty does not clear origin", () => {
    const cache = new _LAILA_IDENTIFIABLE_POOL();
    const origin = new _LAILA_IDENTIFIABLE_POOL();
    cache.proxy_to = origin;
    origin["k"] = "origin_val";
    cache["k"] = "cache_val";
    cache.empty();
    assert.equal(cache.exists("k"), false);
    assert.equal(origin.exists("k"), true);
  });
  test("empty does not clear proxy", () => {
    const cache = new _LAILA_IDENTIFIABLE_POOL();
    const origin = new _LAILA_IDENTIFIABLE_POOL();
    origin.proxy = cache;
    cache["k"] = "cache_val";
    origin.empty();
    assert.equal(cache.exists("k"), true);
  });
});

describe("TestProxyKeys", () => {
  test("keys does not include origin keys", () => {
    const cache = new _LAILA_IDENTIFIABLE_POOL();
    const origin = new _LAILA_IDENTIFIABLE_POOL();
    cache.proxy_to = origin;
    origin["remote"] = "val";
    cache["local"] = "val";
    assert.deepEqual(cache.keys(), ["local"]);
  });
  test("keys after cache fill", () => {
    const cache = new _LAILA_IDENTIFIABLE_POOL();
    const origin = new _LAILA_IDENTIFIABLE_POOL();
    cache.proxy_to = origin;
    origin["remote"] = "val";
    void cache["remote"];
    assert.ok(cache.keys().includes("remote"));
  });
});

describe("TestProxyEdgeCases", () => {
  test("reassign proxy_to", () => {
    const cache = new _LAILA_IDENTIFIABLE_POOL();
    const origin_a = new _LAILA_IDENTIFIABLE_POOL();
    const origin_b = new _LAILA_IDENTIFIABLE_POOL();
    origin_a["k"] = "a_val";
    origin_b["k"] = "b_val";
    cache.proxy_to = origin_a;
    assert.equal(cache["k"], "a_val");
    cache.empty();
    cache.proxy_to = origin_b;
    assert.equal(cache["k"], "b_val");
  });
  test("detach proxy_to", () => {
    const cache = new _LAILA_IDENTIFIABLE_POOL();
    const origin = new _LAILA_IDENTIFIABLE_POOL();
    origin["k"] = "val";
    cache.proxy_to = origin;
    void cache["k"];
    cache.proxy_to = null;
    cache.empty();
    assert.equal(cache["k"], null);
  });
  test("cached value survives origin delete", () => {
    const cache = new _LAILA_IDENTIFIABLE_POOL();
    const origin = new _LAILA_IDENTIFIABLE_POOL();
    origin["k"] = "val";
    cache.proxy_to = origin;
    void cache["k"];
    delete origin["k"];
    assert.equal(cache["k"], "val");
  });
  test("origin update not reflected in stale cache", () => {
    const cache = new _LAILA_IDENTIFIABLE_POOL();
    const origin = new _LAILA_IDENTIFIABLE_POOL();
    origin["k"] = "old";
    cache.proxy_to = origin;
    void cache["k"];
    origin["k"] = "new";
    assert.equal(cache["k"], "old");
  });
  test("four level chain", () => {
    const l1 = new _LAILA_IDENTIFIABLE_POOL();
    const l2 = new _LAILA_IDENTIFIABLE_POOL();
    const l3 = new _LAILA_IDENTIFIABLE_POOL();
    const l4 = new _LAILA_IDENTIFIABLE_POOL();
    l1.__lshift__(l2).__lshift__(l3).__lshift__(l4);
    l4["deep"] = "bottom";
    assert.equal(l1["deep"], "bottom");
    for (const level of [l1, l2, l3, l4]) assert.equal(level.exists("deep"), true);
  });
});

// ---------------------------------------------------------------------------
// test_pool_index.py
// ---------------------------------------------------------------------------
describe("TestPoolIndex", () => {
  const ctx = {};
  use_test_policy(ctx);

  const _pool = (kwargs = {}) => {
    const pool = new _LAILA_IDENTIFIABLE_POOL(kwargs);
    ctx.memory.extend(pool, { pool_nickname: `p-${pool.uuid.slice(0, 8)}` });
    return pool;
  };
  const _memorize = (pool, e) => ctx.memory.memorize(e, { pool_id: pool.global_id }).wait();
  const _three = (pool, nickname = "idx-nick") => {
    const v = laila.variable([0], { nickname });
    const stamps = [];
    _memorize(pool, v);
    stamps.push(v.creation_timestamp);
    for (const i of [1, 2]) {
      time.sleep(0.002);
      v.data = [i];
      _memorize(pool, v);
      stamps.push(v.creation_timestamp);
    }
    return [v, stamps];
  };

  // ---- shard shape and queries -------------------------------------
  t("one shard per base hidden from keys", () => {
    const pool = _pool();
    _three(pool);
    const c = laila.constant("c");
    _memorize(pool, c);
    const visible = pool.keys();
    const raw = [...pool.keys({ include_index: true })];
    assert.equal(visible.length, 4);
    assert.equal(raw.length, 6); // 4 entries + 2 shards (one per base)
    const shards = raw.filter((k) => is_index_key(k));
    assert.equal(shards.length, 2);
    assert.ok(shards.every((k) => k.startsWith("LAILA:POOL_INDEX:")));
    assert.ok(!visible.some((k) => is_index_key(k)));
    assert.ok(![...pool.keys({ as_generator: true })].some((k) => is_index_key(k)));
  });

  t("queries", () => {
    const pool = _pool();
    const [v, stamps] = _three(pool);
    const base = _base(v);
    const idx = pool.index;
    assert.deepEqual(idx.candidates(base), [0, 1, 2].map((i) => `${base}@evolution=${i}`));
    assert.equal(idx.latest(base), `${base}@evolution=2`);
    assert.equal(idx.nth(base, -1), `${base}@evolution=2`);
    assert.equal(idx.nth(base, -3), `${base}@evolution=0`);
    assert.equal(idx.nth(base, -4), null);
    assert.equal(idx.nth(base, 1), `${base}@evolution=1`);
    assert.equal(idx.nth(base, 7), null);
    assert.equal(idx.by_creation_timestamp(base, stamps[1]), `${base}@evolution=1`);
    assert.equal(idx.by_creation_timestamp(base, stamps[1], 1), `${base}@evolution=1`);
    assert.equal(idx.by_creation_timestamp(base, stamps[1], -2), `${base}@evolution=1`);
    assert.equal(idx.by_creation_timestamp(base, stamps[1], 2), null);
    assert.equal(idx.by_creation_timestamp(base, "1970-01-01T00:00:00.000+00:00"), null);
    assert.equal(idx.candidates("LAILA:ENTRY:00000000-0000-0000-0000-000000000000"), null);
  });

  t("constant is indexed and preferred as latest", () => {
    const pool = _pool();
    const c = laila.constant("c", { nickname: "const-nick" });
    _memorize(pool, c);
    const base = c.global_id;
    assert.deepEqual(pool.index.candidates(base), [base]);
    assert.equal(pool.index.latest(base), base);
    assert.equal(pool.index.by_creation_timestamp(base, c.creation_timestamp), base);
  });

  // ---- maintenance --------------------------------------------------
  t("remove updates shard and drops it when empty", () => {
    const pool = _pool();
    const [v] = _three(pool);
    const base = _base(v);
    delete pool[`${base}@evolution=2`];
    assert.equal(pool.index.latest(base), `${base}@evolution=1`);
    delete pool[`${base}@evolution=1`];
    delete pool[`${base}@evolution=0`];
    assert.equal(pool.index.candidates(base), null);
    assert.deepEqual([...pool.keys({ include_index: true })], []); // shard dropped too
  });

  t("latest only shard retention", () => {
    const pool = _pool();
    _three(pool);
    const shards = [...pool.keys({ include_index: true })].filter((k) => is_index_key(k));
    assert.equal(shards.length, 1);
    // Three writes -> shard evolutions 0, 1, 2; only the last survives.
    assert.ok(shards[0].endsWith("@evolution=2"));
  });

  t("rewritten evolution replaces stale timestamp", () => {
    const pool = _pool();
    const a = laila.variable(1, { evolution: 5, uuid: "11111111-1111-4111-8111-111111111111" });
    _memorize(pool, a);
    time.sleep(0.002);
    const b = laila.variable(2, { evolution: 5, uuid: "11111111-1111-4111-8111-111111111111" });
    _memorize(pool, b);
    const base = _base(a);
    const stamps = pool.index._shards[base]["creation_timestamps"];
    assert.deepEqual({ ...stamps }, { [b.creation_timestamp]: 5 });
  });

  t("same timestamp collision keeps highest evolution", () => {
    const pool = _pool();
    const uid = "22222222-2222-4222-8222-222222222222";
    const a = laila.variable(1, { evolution: 0, uuid: uid });
    const b = laila.variable(2, { evolution: 1, uuid: uid });
    b._creation_timestamp = a.creation_timestamp;
    _memorize(pool, a);
    _memorize(pool, b);
    const base = _base(a);
    assert.equal(pool.index.by_creation_timestamp(base, a.creation_timestamp), `${base}@evolution=1`);
  });

  t("index keys are never indexed", () => {
    const pool = _pool();
    _three(pool);
    for (const key of pool.keys({ include_index: true })) {
      if (is_index_key(key)) assert.equal(pool.index.candidates(key.split("@")[0]), null);
    }
  });

  t("flush failure degrades without failing memorize", () => {
    const pool = _pool();
    const v = laila.variable([0], { nickname: "fragile" });
    const proto = _LAILA_IDENTIFIABLE_POOL.prototype;
    const original = proto._write;
    proto._write = function boom(key, value) {
      if (is_index_key(key)) throw new E.RuntimeError("index storage down");
      return original.call(this, key, value);
    };
    try {
      _memorize(pool, v); // must not raise
    } finally {
      proto._write = original;
    }
    assert.equal(pool.exists(v.global_id), true);
    assert.equal(pool.index.candidates(_base(v)), null); // shard was invalidated
    // ... and remember still works through the scan fallback.
    const r = ctx.memory.remember("ENTRY:fragile", { pool_id: pool.global_id, persist: false }).wait();
    assert.deepEqual(r.data, [0]);
  });

  // ---- persistence ----------------------------------------------------
  t("foreign index pool keeps data pool clean", () => {
    const ram = new _LAILA_IDENTIFIABLE_POOL();
    const data = _pool({ index_pool: ram });
    _three(data, "foreign-nick");
    assert.equal([...data.keys({ include_index: true })].length, 3);
    const shards = [...ram.keys({ include_index: true })];
    assert.equal(shards.length, 1);
    assert.ok(is_index_key(shards[0]));
    const r = ctx.memory.remember("ENTRY:foreign-nick@evolution=-1", { pool_id: data.global_id, persist: false }).wait();
    assert.equal(r.evolution, 2);
    data.empty();
    assert.deepEqual([...ram.keys({ include_index: true })], []);
    assert.deepEqual([...data.keys({ include_index: true })], []);
  });

  t("shard reloads from storage in fresh pool object", () => {
    const first = new SQLitePool();
    ctx.memory.extend(first, { pool_nickname: "sq-first" });
    let second = null;
    try {
      const [v, stamps] = _three(first, "persist-nick");
      const base = _base(v);
      // A new object over the same storage: empty in-memory index.
      second = new SQLitePool({ uuid: first.uuid });
      ctx.memory.extend(second, { pool_nickname: "sq-second" });
      assert.deepEqual({ ...second.index._shards }, {});
      const scans = { n: 0 };
      const raw_keys = second._keys.bind(second);
      second._keys = (...a) => {
        scans.n += 1;
        return raw_keys(...a);
      };
      let r = ctx.memory.remember("ENTRY:persist-nick@evolution=-1", { pool_id: second.global_id, persist: false }).wait();
      assert.equal(r.evolution, 2);
      r = ctx.memory.remember(`ENTRY:persist-nick@creation_timestamp=${stamps[0]}`, { pool_id: second.global_id, persist: false }).wait();
      assert.equal(r.evolution, 0);
      // Loading the shard costs one candidate scan; the lookups themselves none.
      assert.equal(scans.n, 1);
      assert.equal(second.index.latest(base), v.global_id);
    } finally {
      for (const k of [...first.keys({ include_index: true })]) first._delete(k);
      first.close();
      if (second) second.close();
    }
  });

  t("rebuild over unindexed keys", () => {
    const pool = _pool({ index_enabled: false });
    const [v, stamps] = _three(pool, "rebuild-nick");
    assert.deepEqual([...pool.keys({ include_index: true })], sorted(pool.keys()));
    pool.index_enabled = true;
    const base = _base(v);
    assert.equal(pool.index.candidates(base), null);
    pool.index.rebuild();
    assert.equal(pool.index.latest(base), `${base}@evolution=2`);
    assert.equal(pool.index.by_creation_timestamp(base, stamps[1]), `${base}@evolution=1`);
    // Rebuild replaces stale contents outright.
    pool._delete(`${base}@evolution=2`);
    pool.index.rebuild();
    assert.equal(pool.index.latest(base), `${base}@evolution=1`);
  });

  t("shard id is deterministic per owner", () => {
    const a = new _LAILA_IDENTIFIABLE_POOL({ uuid: "33333333-3333-4333-8333-333333333333" });
    const b = new _LAILA_IDENTIFIABLE_POOL({ uuid: "44444444-4444-4444-8444-444444444444" });
    const base = "LAILA:ENTRY:55555555-5555-4555-8555-555555555555";
    assert.equal(new PoolIndex(a).shard_id(base), new PoolIndex(a).shard_id(base));
    assert.notEqual(new PoolIndex(a).shard_id(base), new PoolIndex(b).shard_id(base));
    assert.ok(is_index_key(new PoolIndex(a).shard_id(base)));
  });
});

// ---------------------------------------------------------------------------
// test_data_container.py
// ---------------------------------------------------------------------------
describe("TestDataContainerIsVirtual", () => {
  test("instantiable as a type", () => {
    assert.ok(new _LAILA_IDENTIFIABLE_DATA_CONTAINER() instanceof _LAILA_IDENTIFIABLE_DATA_CONTAINER);
  });
  test("getitem raises not implemented", () => {
    const container = new _LAILA_IDENTIFIABLE_DATA_CONTAINER();
    assert.throws(() => container["k"], E.NotImplementedError);
  });
  test("setitem raises not implemented", () => {
    const container = new _LAILA_IDENTIFIABLE_DATA_CONTAINER();
    assert.throws(() => {
      container["k"] = 1;
    }, E.NotImplementedError);
  });
  test("empty raises not implemented", () => {
    assert.throws(() => new _LAILA_IDENTIFIABLE_DATA_CONTAINER().empty(), E.NotImplementedError);
  });
  test("error message names the concrete class", () => {
    class Half extends _LAILA_IDENTIFIABLE_DATA_CONTAINER {}
    assert.throws(() => new Half()[0], (e) => e instanceof E.NotImplementedError && /Half/.test(String(e)));
  });
});

describe("TestDataContainerIdentity", () => {
  test("scope is data container", () => {
    assert.deepEqual(new _LAILA_IDENTIFIABLE_DATA_CONTAINER()._scopes, [_DATA_CONTAINER_SCOPE]);
    assert.equal(_DATA_CONTAINER_SCOPE, "DATA_CONTAINER");
  });
  test("global id carries scope", () => {
    assert.ok(new _LAILA_IDENTIFIABLE_DATA_CONTAINER().global_id.includes(_DATA_CONTAINER_SCOPE));
  });
  test("nickname derived uuid is deterministic", () => {
    const a = new _LAILA_IDENTIFIABLE_DATA_CONTAINER({ nickname: "dc-nick" });
    const b = new _LAILA_IDENTIFIABLE_DATA_CONTAINER({ nickname: "dc-nick" });
    assert.equal(a.uuid, b.uuid);
  });
  test("atomic lock available", () => {
    const container = new _LAILA_IDENTIFIABLE_DATA_CONTAINER();
    with_(container.atomic(), () => {
      with_(container.atomic(), () => {});
    });
  });
});

describe("TestHierarchy", () => {
  test("pool is a data container", () => {
    assert.ok(_LAILA_IDENTIFIABLE_POOL.prototype instanceof _LAILA_IDENTIFIABLE_DATA_CONTAINER);
    assert.ok(new _LAILA_IDENTIFIABLE_POOL() instanceof _LAILA_IDENTIFIABLE_DATA_CONTAINER);
  });
  test("multibuffer is a data container", () => {
    assert.ok(MultiBuffer.prototype instanceof _LAILA_IDENTIFIABLE_DATA_CONTAINER);
    assert.ok(new MultiBuffer() instanceof _LAILA_IDENTIFIABLE_DATA_CONTAINER);
  });
  test("multibuffer is not a pool", () => {
    assert.ok(!(MultiBuffer.prototype instanceof _LAILA_IDENTIFIABLE_POOL));
  });
  test("subclasses keep their own scopes", () => {
    assert.deepEqual(new _LAILA_IDENTIFIABLE_POOL()._scopes, [_POOL_SCOPE]);
    assert.deepEqual(new MultiBuffer()._scopes, [_MULTI_BUFFER_SCOPE]);
    assert.equal(_MULTI_BUFFER_SCOPE, "MULTI_BUFFER");
  });
  test("pool overrides are concrete", () => {
    const pool = new _LAILA_IDENTIFIABLE_POOL();
    pool["k"] = "v";
    assert.equal(pool["k"], "v");
    pool.empty();
    assert.equal(pool["k"], null);
  });
  test("default pool alias still points at pool", () => {
    assert.equal(laila.DefaultPool, _LAILA_IDENTIFIABLE_POOL);
    assert.equal(laila.DefaultMultiBuffer, MultiBuffer);
  });
});

// ---------------------------------------------------------------------------
// test_multibuffer.py
// ---------------------------------------------------------------------------
describe("TestMultiBufferConstruction", () => {
  test("default is a double buffer", () => {
    const mb = new MultiBuffer();
    assert.equal(mb.capacity, 2);
    assert.equal(mb.__len__(), 2);
    assert.deepEqual(mb.slots, [null, null]);
    assert.equal(mb.mapped, false);
  });
  test("explicit capacity sizes slots", () => {
    assert.deepEqual(new MultiBuffer({ capacity: 4 }).slots, [null, null, null, null]);
  });
  test("capacity must be positive", () => {
    assert.throws(() => new MultiBuffer({ capacity: 0 }));
  });
  test("explicit slots used by reference", () => {
    const external = [null, null, null];
    assert.equal(new MultiBuffer({ slots: external }).slots, external);
  });
  test("explicit slots override capacity", () => {
    const mb = new MultiBuffer({ slots: [null, null, null], capacity: 7 });
    assert.equal(mb.capacity, 3);
    assert.equal(mb.__len__(), 3);
  });
  test("empty slots rejected", () => {
    assert.throws(() => new MultiBuffer({ slots: [] }), E.ValueError);
  });
  test("unsized slots rejected", () => {
    assert.throws(() => new MultiBuffer({ slots: 42 }), E.TypeError);
  });
  test("heads start at zero", () => {
    const mb = new MultiBuffer();
    assert.equal(mb.read_head, 0);
    assert.equal(mb.write_head, 0);
  });
  test("heads are read only", () => {
    const mb = new MultiBuffer();
    assert.throws(() => {
      mb.read_head = 1;
    });
  });
});

describe("TestMultiBufferItemProtocol", () => {
  test("setitem entry stores record", () => {
    const mb = new MultiBuffer();
    const entry = laila.constant("a");
    mb[0] = entry;
    assert.ok(mb.slots[0] instanceof Record);
    assert.equal(mb.slots[0].entry, entry);
  });
  test("setitem record stored as is", () => {
    const mb = new MultiBuffer();
    const record = new Record({ entry: laila.constant("a") });
    mb[1] = record;
    assert.equal(mb.slots[1], record);
  });
  test("setitem raw payload lifted to record of constant", () => {
    const mb = new MultiBuffer();
    mb[0] = B("\xde\xad");
    assert.ok(mb.slots[0] instanceof Record);
    assert.ok(mb.slots[0].entry instanceof Entry);
    assert.ok(Buffer.from(mb.slots[0].entry.data).equals(B("\xde\xad")));
  });
  test("setitem none clears slot", () => {
    const mb = new MultiBuffer();
    mb[0] = laila.constant("a");
    mb[0] = null;
    assert.equal(mb.slots[0], null);
  });
  test("getitem returns entry not record", () => {
    const mb = new MultiBuffer();
    const entry = laila.constant("a");
    mb[0] = entry;
    const out = mb[0];
    assert.ok(out instanceof Entry);
    assert.ok(!(out instanceof Record));
    assert.equal(out, entry);
  });
  test("getitem empty slot is none", () => {
    assert.equal(new MultiBuffer()[0], null);
  });
  test("getitem raw slot wrapped into constant entry", () => {
    const mb = new MultiBuffer();
    mb.slots[1] = B("raw-frame");
    const out = mb[1];
    assert.ok(out instanceof Entry);
    assert.ok(Buffer.from(out.data).equals(B("raw-frame")));
    assert.equal(out.evolution, null);
  });
  test("getitem bare entry in slot passes through", () => {
    const mb = new MultiBuffer();
    const entry = laila.constant("a");
    mb.slots[0] = entry;
    assert.equal(mb[0], entry);
  });
  test("index wraps modulo capacity", () => {
    const mb = new MultiBuffer({ capacity: 3 });
    const entry = laila.constant("w");
    mb[4] = entry;
    assert.equal(mb[1], entry);
    assert.equal(mb[-2], entry);
    assert.equal(mb.slots[1].entry, entry);
  });
  test("non integer index rejected", () => {
    const mb = new MultiBuffer();
    // ``mb["0"]`` is indistinguishable from ``mb[0]`` in JS (property keys
    // are strings); the explicit protocol call carries the type.
    assert.throws(() => mb.__getitem__("0"), E.TypeError);
    assert.throws(() => {
      mb[true] = laila.constant("x");
    }, E.TypeError);
    assert.throws(() => mb.__getitem__(0.5), E.TypeError);
  });
});

describe("TestMultiBufferReadWrite", () => {
  test("write goes through setitem and advances", () => {
    const mb = new MultiBuffer({ capacity: 3 });
    const idx = mb.write(laila.constant("a"));
    assert.equal(idx, 0);
    assert.ok(mb.slots[0] instanceof Record);
    assert.equal(mb.write_head, 1);
    assert.equal(mb.read_head, 0);
  });
  test("read goes through getitem and advances", () => {
    const mb = new MultiBuffer({ capacity: 3 });
    const entry = laila.constant("a");
    mb.write(entry);
    assert.equal(mb.read(), entry);
    assert.equal(mb.read_head, 1);
    assert.equal(mb.write_head, 1);
  });
  test("read returns what is at read head", () => {
    const mb = new MultiBuffer({ capacity: 3 });
    const entries = [0, 1, 2].map((i) => laila.constant(i));
    for (const e of entries) mb.write(e);
    assert.equal(mb.read(), entries[0]);
    assert.equal(mb.read(), entries[1]);
    assert.equal(mb.read(), entries[2]);
  });
  test("read of empty slot returns none and advances", () => {
    const mb = new MultiBuffer();
    assert.equal(mb.read(), null);
    assert.equal(mb.read_head, 1);
  });
  test("heads wrap independently", () => {
    const mb = new MultiBuffer({ capacity: 2 });
    const [a, b, c] = ["a", "b", "c"].map((x) => laila.constant(x));
    mb.write(a);
    mb.write(b);
    assert.equal(mb.write_head, 0);
    assert.equal(mb.read_head, 0);
    mb.write(c); // overwrites slot 0
    assert.equal(mb.write_head, 1);
    assert.equal(mb.read(), c);
    assert.equal(mb.read(), b);
    assert.equal(mb.read_head, 0);
  });
  test("double buffer producer consumer handoff", () => {
    const mb = new MultiBuffer();
    for (let i = 0; i < 10; i++) {
      mb.write(laila.constant(i));
      assert.equal(mb.read().data, i);
      assert.equal(mb.read_head, mb.write_head);
    }
  });
  test("write requires value when not mapped", () => {
    assert.throws(() => new MultiBuffer().write(), E.TypeError);
  });
  test("write raw payload when not mapped is recorded", () => {
    const mb = new MultiBuffer();
    mb.write(B("frame"));
    assert.ok(mb.slots[0] instanceof Record);
    assert.ok(Buffer.from(mb.read().data).equals(B("frame")));
  });
  test("uses setitem and getitem overrides", () => {
    const calls = [];
    class Spy extends MultiBuffer {
      __setitem__(key, value) {
        calls.push(["set", key]);
        super.__setitem__(key, value);
      }
      __getitem__(key) {
        calls.push(["get", key]);
        return super.__getitem__(key);
      }
    }
    const spy = new Spy({ capacity: 2 });
    spy.write(laila.constant(1));
    spy.read();
    assert.deepEqual(calls, [
      ["set", 0],
      ["get", 0],
    ]);
  });
});

describe("TestMultiBufferEmpty", () => {
  test("empty clears slots and rewinds heads", () => {
    const mb = new MultiBuffer({ capacity: 3 });
    mb.write(laila.constant(1));
    mb.write(laila.constant(2));
    mb.read();
    mb.empty();
    assert.deepEqual(mb.slots, [null, null, null]);
    assert.equal(mb.read_head, 0);
    assert.equal(mb.write_head, 0);
  });
  test("empty mutates external slots in place", () => {
    const external = [B("a"), B("b")];
    const mb = new MultiBuffer({ slots: external, mapped: true });
    mb.empty();
    assert.deepEqual(external, [null, null]);
  });
});

describe("TestMultiBufferMappedMode", () => {
  test("write bypasses setitem", () => {
    const calls = [];
    class Spy extends MultiBuffer {
      __setitem__(key, value) {
        calls.push(key);
        super.__setitem__(key, value);
      }
    }
    const dma = [null, null];
    const spy = new Spy({ slots: dma, mapped: true });
    dma[0] = B("\x00");
    const idx = spy.write();
    assert.equal(idx, 0);
    assert.deepEqual(calls, []);
    assert.equal(spy.write_head, 1);
    assert.ok(dma[0].equals(B("\x00")));
  });
  test("write with value deposits raw bytes", () => {
    const dma = [null, null];
    const mb = new MultiBuffer({ slots: dma, mapped: true });
    mb.write(B("\x01\x02"));
    assert.ok(dma[0].equals(B("\x01\x02")));
    assert.ok(!(dma[0] instanceof Record));
  });
  test("read wraps hardware bytes into entry", () => {
    const dma = [null, null];
    const mb = new MultiBuffer({ slots: dma, mapped: true });
    dma[0] = B("frame-0");
    mb.write();
    const out = mb.read();
    assert.ok(out instanceof Entry);
    assert.ok(Buffer.from(out.data).equals(B("frame-0")));
    assert.equal(mb.read_head, 1);
  });
  test("externally mutated slot visible without write", () => {
    const dma = [null, null];
    const mb = new MultiBuffer({ slots: dma, mapped: true });
    dma[0] = B("late");
    assert.ok(Buffer.from(mb.read().data).equals(B("late")));
  });
  test("bytearray backed slots", () => {
    const dma = [new PyByteArray(2), new PyByteArray(2)];
    const mb = new MultiBuffer({ slots: dma, mapped: true });
    dma[0].set(B("\xaa\xbb"));
    mb.write();
    assert.ok(Buffer.from(mb.read().data).equals(B("\xaa\xbb")));
  });
  t("camera loop memorize remember roundtrip", () => {
    const policy = laila.get_active_policy();
    const dma = [null, null];
    const frames = new MultiBuffer({ slots: dma, mapped: true });

    const captured = [];
    for (let i = 0; i < 4; i++) {
      dma[frames.write_head] = Buffer.alloc(4, i); // "hardware" fills the slot
      frames.write();
      const entry = frames.read();
      policy.future_bank[laila.memorize(entry).global_id].wait();
      captured.push(entry);
    }

    captured.forEach((entry, i) => {
      let ref;
      with_(laila.guarantee, () => {
        ref = laila.remember(entry.global_id);
      });
      const back = policy.future_bank[ref.global_id].result;
      assert.equal(back.global_id, entry.global_id);
      assert.ok(Buffer.from(back.data).equals(Buffer.alloc(4, i)));
    });
  });
});

describe("TestMultiBufferCliContract", () => {
  test("slots is cli exempt", () => {
    const eligible = [..._eligible_model_fields(MultiBuffer)];
    assert.ok(!eligible.includes("slots"));
    assert.ok(eligible.includes("capacity"));
    assert.ok(eligible.includes("mapped"));
  });
  test("explicit uuid respected", () => {
    const mb = new MultiBuffer({ uuid: "12345678-1234-5678-1234-567812345678" });
    assert.equal(mb.uuid, "12345678-1234-5678-1234-567812345678");
  });
  test("global id has multibuffer scope", () => {
    assert.ok(new MultiBuffer().global_id.includes("MULTI_BUFFER"));
  });
});

// ---------------------------------------------------------------------------
// test_memory_pool.py
// ---------------------------------------------------------------------------
describe("TestMemoryPool", () => {
  let pool;
  beforeEach(() => {
    pool = new _LAILA_IDENTIFIABLE_POOL();
  });

  test("01 pool id format", () => assert.ok(pool.pool_id.startsWith("LAILA:POOL:")));
  test("02 resource starts empty", () => {
    assert.deepEqual({ ...pool.resource }, {});
    assert.equal(pool.exists("missing"), false);
  });
  test("03 set and get roundtrip", () => {
    pool["a"] = 123;
    assert.equal(pool["a"], 123);
  });
  test("04 get missing returns none", () => assert.equal(pool["nope"], null));
  test("05 set none and get returns none but exists true", () => {
    pool["n"] = null;
    assert.equal(pool.exists("n"), true);
    assert.equal(pool["n"], null);
  });
  test("06 delitem silent if missing", () => {
    pool.__delitem__("ghost");
    assert.equal(pool.exists("ghost"), false);
  });
  test("07 delitem removes existing", () => {
    pool["x"] = "val";
    assert.equal(pool.exists("x"), true);
    delete pool["x"];
    assert.equal(pool.exists("x"), false);
    assert.equal(pool["x"], null);
  });
  test("08 store entry by global id", () => {
    const e = laila.constant(42);
    pool[e.global_id] = e;
    assert.equal(pool[e.global_id], e);
  });
  test("09 getitem roundtrip", () => {
    pool["k"] = "v";
    assert.equal(pool["k"], "v");
    assert.equal(pool["missing"], null);
  });
  test("10 exists true when key present", () => {
    pool["present"] = 7;
    assert.equal(pool.exists("present"), true);
  });
  test("11 exists false when key absent", () => assert.equal(pool.exists("absent"), false));
  test("12 keys snapshot returns list", () => {
    pool["a"] = 1;
    pool["b"] = 2;
    const ks = pool.keys({ as_generator: false });
    assert.ok(Array.isArray(ks));
    count_equal(ks, ["a", "b"]);
  });
  test("13 keys snapshot is not affected by later mutations", () => {
    pool["a"] = 1;
    const snap = pool.keys();
    pool["b"] = 2;
    count_equal(snap, ["a"]);
  });
  test("14 keys generator yields all current keys", () => {
    const items = Object.fromEntries(Array.from({ length: 10 }, (_, i) => [`k${i}`, i]));
    for (const [k, v] of Object.entries(items)) pool[k] = v;
    const it = pool.keys({ as_generator: true });
    assert.equal(typeof it[Symbol.iterator], "function");
    count_equal([...it], Object.keys(items));
  });
  // 15: "mutating dict while iterating raises RuntimeError" is CPython dict
  // behaviour; JS object key iteration is snapshot-based so the generator
  // simply yields the snapshot. Covered by 28 below.
  test("16 reentrancy of atomic", () => {
    with_(pool.atomic(), () => {
      pool["r"] = 1;
      assert.equal(pool["r"], 1);
    });
  });
  test("17 multiple instances have independent resources", () => {
    const p1 = new _LAILA_IDENTIFIABLE_POOL();
    const p2 = new _LAILA_IDENTIFIABLE_POOL();
    p1["x"] = 1;
    p2["x"] = 2;
    assert.equal(p1["x"], 1);
    assert.equal(p2["x"], 2);
  });
  test("18 overwrite value", () => {
    pool["k"] = "v1";
    pool["k"] = "v2";
    assert.equal(pool["k"], "v2");
  });
  test("19 types any value", () => {
    const complex_value = { a: [1, 2, { b: "c" }] };
    pool["complex"] = complex_value;
    assert.deepEqual(pool["complex"], complex_value);
  });
  t("20 concurrent writes no loss", () => {
    const N = 50;
    const threads = [];
    for (let i = 0; i < N; i++) {
      const th = new TH.Thread({ target: (j) => void (pool[String(j)] = j), args: [i] });
      th.start();
      threads.push(th);
    }
    for (const th of threads) th.join();
    for (let i = 0; i < N; i++) assert.equal(pool[String(i)], i);
  });
  t("21 concurrent writes and reads", () => {
    // Bounded version of the 0.2s writer/reader race: cooperative threads
    // cannot spin on a stop flag, so each side runs a fixed number of rounds.
    const read_errors = [];
    const writer = () => {
      for (let n = 0; n < 500; n++) pool[String(n % 10)] = n;
    };
    const reader = () => {
      for (let n = 0; n < 50; n++) {
        for (const k of [...pool.keys()]) {
          const v = pool[k];
          if (v !== null && typeof v !== "number" && !(v instanceof Entry)) read_errors.push(`bad value ${v}`);
        }
      }
    };
    const tw = new TH.Thread({ target: writer });
    const tr = new TH.Thread({ target: reader });
    tw.start();
    tr.start();
    tw.join();
    tr.join();
    assert.deepEqual(read_errors, []);
  });
  test("22 keys generator partial consumption then close", () => {
    for (let i = 0; i < 5; i++) pool[String(i)] = i;
    const it = pool.keys({ as_generator: true });
    const first = it.next().value;
    assert.ok(["0", "1", "2", "3", "4"].includes(first));
    it.return();
  });
  test("23 store entry and exists", () => {
    const e = laila.constant({ x: 1 });
    pool[e.global_id] = e;
    assert.equal(pool.exists(e.global_id), true);
  });
  test("24 getitem missing", () => assert.equal(pool["no-such"], null));
  test("25 delete then recreate", () => {
    pool["z"] = 9;
    delete pool["z"];
    assert.equal(pool.exists("z"), false);
    pool["z"] = 10;
    assert.equal(pool["z"], 10);
  });
  test("26 keys snapshot type and contents after many mutations", () => {
    for (let i = 0; i < 20; i++) pool[String(i)] = i;
    for (let i = 0; i < 10; i++) delete pool[String(i)];
    const snap = pool.keys();
    assert.ok(Array.isArray(snap));
    count_equal(
      snap,
      Array.from({ length: 10 }, (_, i) => String(i + 10)),
    );
  });
  test("27 atomic context manager usable directly", () => {
    with_(pool.atomic(), () => {
      pool.resource["inside"] = true;
    });
    assert.equal(pool.exists("inside"), true);
    assert.equal(pool["inside"], true);
  });
  test("28 generator exhaustion allows future writes", () => {
    for (let i = 0; i < 5; i++) pool[String(i)] = i;
    void [...pool.keys({ as_generator: true })];
    pool["new"] = "ok";
    assert.equal(pool["new"], "ok");
  });
  t("29 many concurrent store entry", () => {
    const entries = Array.from({ length: 50 }, (_, i) => laila.constant(`load-${i}`));
    const threads = entries.map((e) => new TH.Thread({ target: (x) => void (pool[x.global_id] = x), args: [e] }));
    for (const th of threads) th.start();
    for (const th of threads) th.join();
    for (const e of entries) assert.ok(pool[e.global_id] instanceof Entry);
  });
  test("30 exists after set none", () => {
    pool["maybe"] = null;
    assert.equal(pool.exists("maybe"), true);
    assert.equal(pool["maybe"], null);
  });
});

// ---------------------------------------------------------------------------
// test_memory_compdata_roundtrip.py
// ---------------------------------------------------------------------------
describe("TestMemoryPoolCompDataRoundtrip", () => {
  RT.register_dtype_matrix(() => new _LAILA_IDENTIFIABLE_POOL(), { store_as_json_string: false });
});

// Tear down the lazily-activated default policy so the process exits.
test("zz teardown", () =>
  macrotask(() => {
    laila.terminate();
    fs.rmSync(TMP_ROOT, { recursive: true, force: true });
  }));
