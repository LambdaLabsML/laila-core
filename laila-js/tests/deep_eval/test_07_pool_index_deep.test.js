/**
 * Port of ``tests/deep_eval/test_07_pool_index_deep.py``.
 *
 * Deep tests for ``PoolIndex`` -- shard maintenance and reference resolution.
 */
import { test, describe, beforeEach, afterEach } from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { S, laila, macrotask, with_fresh_policy_async as with_fresh_policy } from "./_fixtures.js";

const E = await import(S + "_compat/errors.js");
const time = await import(S + "_compat/time.js");
const PI = await import(S + "data/schema/pool_index.js");
const { CREATION_TIMESTAMP_ATTRIBUTE, PoolIndex, _key_evolution, is_index_key } = PI;
const { _evolution_key, _rank } = PI;
const { SQLitePool } = await import(S + "data/sqlite/sqlite.js");
const { Entry } = await import(S + "entry/entry.js");
const { DefaultPool } = await import(S + "macros/defaults.js");
const { Record } = await import(S + "policy/central/memory/record/record.js");

const U1 = "11111111-2222-3333-4444-555555555555";
const BASE = `LAILA:ENTRY:${U1}`;

const TMP_ROOT = fs.mkdtempSync(path.join(os.tmpdir(), "laila_deep07_"));
laila.set_default_directory(TMP_ROOT);

const t = (name, fn) => test(name, () => macrotask(fn));
const sorted = (xs) => [...xs].sort();
const tmp_path = () => fs.mkdtempSync(path.join(TMP_ROOT, "tmp_"));
const range = (n) => Array.from({ length: n }, (_, i) => i);

function _store(pool, entry) {
  const rec = new Record({ entry });
  const blob = pool.transformations !== null && pool.transformations !== undefined ? rec.serialize(pool.transformations) : rec.as_dict;
  pool[entry.global_id] = blob;
  return entry.global_id;
}

function _store_versions(pool, n, nickname = "ix") {
  let e = Entry.variable(0, { nickname });
  const base = e.global_id.split("@")[0];
  const stamps = [];
  for (const i of range(n)) {
    _store(pool, e);
    stamps.push(e.creation_timestamp);
    time.sleep(0.002);
    e = e.evolve(i + 1);
  }
  return [base, stamps];
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

describe("TestHelpers", () => {
  t("test_is_index_key", () => {
    assert.ok(is_index_key("LAILA:POOL_INDEX:" + U1));
    assert.ok(is_index_key("LAILA:POOL_INDEX:" + U1 + "@evolution=1"));
    assert.ok(!is_index_key(BASE));
    assert.ok(!is_index_key("LAILA:POOL:" + U1));
  });

  for (const [key, expected] of [
    [BASE, null],
    [`${BASE}@evolution=3`, 3],
    [`${BASE}@evolution=-1`, null],
    [`${BASE}@evolution=x`, null],
  ]) {
    t(`test_key_evolution[${key}-${expected}]`, () => {
      assert.equal(_key_evolution(key), expected);
    });
  }

  test(
    "test_evolution_key",
    () => {
      assert.equal(_evolution_key(BASE, null), BASE);
      assert.equal(_evolution_key(BASE, 4), `${BASE}@evolution=4`);
    },
  );

  test(
    "test_rank_constant_lowest",
    () => {
      assert.ok(_rank(null) < _rank(0) && _rank(0) < _rank(1));
    },
  );

  t("test_constant_name", () => {
    assert.equal(CREATION_TIMESTAMP_ATTRIBUTE, "creation_timestamp");
  });
});

// ---------------------------------------------------------------------------
// Shard maintenance
// ---------------------------------------------------------------------------

for (const backend of ["default", "sqlite"]) {
  describe(`TestShards[${backend}]`, () => {
    let pool;
    beforeEach(() =>
      macrotask(() => {
        pool = backend === "default" ? new DefaultPool() : new SQLitePool({ file_path: path.join(tmp_path(), "ix.sqlite") });
      }),
    );
    afterEach(() =>
      macrotask(() => {
        if (typeof pool?.close === "function") pool.close();
      }),
    );

    t("test_index_lazy", () => {
      assert.equal(pool._index, null);
      const idx = pool.index;
      assert.ok(idx instanceof PoolIndex);
      assert.equal(pool.index, idx);
    });

    t("test_unindexed_base_none", () => {
      assert.equal(pool.index.candidates(BASE), null);
      assert.equal(pool.index.latest(BASE), null);
      assert.equal(pool.index.nth(BASE, 0), null);
    });

    t("test_record_variable_versions", () => {
      const [base] = _store_versions(pool, 3);
      assert.deepEqual(
        pool.index.candidates(base),
        range(3).map((i) => `${base}@evolution=${i}`),
      );
      assert.equal(pool.index.latest(base), `${base}@evolution=2`);
    });

    t("test_record_constant", () => {
      const e = Entry.constant(1, { uuid: U1 });
      _store(pool, e);
      assert.deepEqual(pool.index.candidates(BASE), [BASE]);
      assert.equal(pool.index.latest(BASE), BASE);
      assert.equal(pool.index.nth(BASE, -1), BASE);
    });

    t("test_constant_and_variable_same_base", () => {
      _store(pool, Entry.constant(1, { uuid: U1 }));
      _store(pool, Entry.variable(2, { uuid: U1, evolution: 0 }));
      const cands = pool.index.candidates(BASE);
      assert.deepEqual(cands, [BASE, `${BASE}@evolution=0`]);
      assert.equal(pool.index.nth(BASE, -1), `${BASE}@evolution=0`);
    });

    t("test_nth_positive_exact", () => {
      const [base] = _store_versions(pool, 3);
      assert.equal(pool.index.nth(base, 1), `${base}@evolution=1`);
      assert.equal(pool.index.nth(base, 7), null);
    });

    t("test_nth_negative", () => {
      const [base] = _store_versions(pool, 3);
      assert.equal(pool.index.nth(base, -1), `${base}@evolution=2`);
      assert.equal(pool.index.nth(base, -3), `${base}@evolution=0`);
      assert.equal(pool.index.nth(base, -4), null);
    });

    t("test_out_of_order_writes_sorted", () => {
      for (const ev of [5, 1, 3]) _store(pool, Entry.variable(ev, { uuid: U1, evolution: ev }));
      assert.deepEqual(
        pool.index.candidates(BASE),
        [1, 3, 5].map((e) => `${BASE}@evolution=${e}`),
      );
      assert.equal(pool.index.nth(BASE, -2), `${BASE}@evolution=3`);
    });

    t("test_duplicate_write_not_duplicated", () => {
      const e = Entry.variable(1, { uuid: U1 });
      _store(pool, e);
      _store(pool, e);
      assert.deepEqual(pool.index.candidates(BASE), [`${BASE}@evolution=0`]);
    });

    t("test_remove", () => {
      const [base] = _store_versions(pool, 3);
      delete pool[`${base}@evolution=1`];
      assert.deepEqual(pool.index.candidates(base), [`${base}@evolution=0`, `${base}@evolution=2`]);
    });

    t("test_remove_last_drops_shard", () => {
      _store(pool, Entry.constant(1, { uuid: U1 }));
      delete pool[BASE];
      const c = pool.index.candidates(BASE);
      assert.ok(c === null || (Array.isArray(c) && c.length === 0));
    });

    t("test_by_creation_timestamp", () => {
      const [base, stamps] = _store_versions(pool, 3);
      assert.equal(pool.index.by_creation_timestamp(base, stamps[1]), `${base}@evolution=1`);
      assert.equal(pool.index.by_creation_timestamp(base, stamps[1], 1), `${base}@evolution=1`);
      assert.equal(pool.index.by_creation_timestamp(base, stamps[1], 0), null);
      assert.equal(pool.index.by_creation_timestamp(base, "1999-01-01T00:00:00.000+00:00"), null);
    });

    t("test_by_creation_timestamp_negative_evolution", () => {
      const [base, stamps] = _store_versions(pool, 3);
      assert.equal(pool.index.by_creation_timestamp(base, stamps[2], -1), `${base}@evolution=2`);
    });

    t("test_resolve_indexed", () => {
      const [base, stamps] = _store_versions(pool, 3);
      assert.equal(pool._resolve_indexed(base, { evolution: "-1" }), `${base}@evolution=2`);
      assert.equal(pool._resolve_indexed(base, {}), `${base}@evolution=2`);
      assert.equal(pool._resolve_indexed(base, { evolution: "0" }), `${base}@evolution=0`);
      assert.equal(pool._resolve_indexed(base, { creation_timestamp: stamps[0] }), `${base}@evolution=0`);
    });

    t("test_resolve_indexed_disabled", () => {
      const [base] = _store_versions(pool, 2);
      pool.index_enabled = false;
      assert.equal(pool._resolve_indexed(base, { evolution: "-1" }), null);
    });

    t("test_invalidate_reloads_from_storage", () => {
      const [base] = _store_versions(pool, 2);
      pool.index.invalidate(base);
      assert.deepEqual(pool.index.candidates(base), [`${base}@evolution=0`, `${base}@evolution=1`]);
    });

    t("test_invalidate_all", () => {
      const [base] = _store_versions(pool, 2);
      pool.index.invalidate();
      assert.deepEqual(pool.index.candidates(base), [`${base}@evolution=0`, `${base}@evolution=1`]);
    });

    t("test_rebuild_after_out_of_band_write", () => {
      const [base] = _store_versions(pool, 2);
      const e = Entry.variable(9, { uuid: base.split(":").pop(), evolution: 9 });
      const rec = new Record({ entry: e });
      const blob = pool.transformations !== null && pool.transformations !== undefined ? rec.serialize(pool.transformations) : rec.as_dict;
      pool._write(e.global_id, blob); // bypass index
      assert.ok(!pool.index.candidates(base).includes(`${base}@evolution=9`));
      pool.index.rebuild();
      assert.ok(pool.index.candidates(base).includes(`${base}@evolution=9`));
    });

    t("test_rebuild_drops_stale", () => {
      const [base] = _store_versions(pool, 2);
      pool._delete(`${base}@evolution=1`); // bypass index
      pool.index.rebuild();
      assert.deepEqual(pool.index.candidates(base), [`${base}@evolution=0`]);
    });

    t("test_clear", () => {
      _store_versions(pool, 2);
      pool.index.clear();
      assert.deepEqual(
        [...pool.keys({ include_index: true })].filter((k) => is_index_key(k)),
        [],
      );
    });

    t("test_shard_persisted_in_pool", () => {
      _store_versions(pool, 1);
      const idx_keys = [...pool.keys({ include_index: true })].filter((k) => is_index_key(k));
      assert.equal(idx_keys.length, 1);
      assert.ok(idx_keys[0].startsWith("LAILA:POOL_INDEX:"));
    });

    t("test_shard_id_deterministic", () => {
      assert.equal(pool.index.shard_id(BASE), pool.index.shard_id(BASE));
      const other = new DefaultPool();
      assert.notEqual(pool.index.shard_id(BASE), other.index.shard_id(BASE));
    });

    t("test_index_pool_separate", () => {
      const store = new DefaultPool();
      const main = new DefaultPool({ index_pool: store });
      _store(main, Entry.constant(1, { uuid: U1 }));
      assert.deepEqual([...main.keys({ include_index: true })], [BASE]);
      assert.ok([...store.keys({ include_index: true })].some((k) => is_index_key(k)));
      assert.deepEqual(main.index.candidates(BASE), [BASE]);
      main.empty();
      assert.deepEqual([...store.keys({ include_index: true })], []);
    });

    t("test_index_disabled_no_shards", () => {
      const p = new DefaultPool({ index_enabled: false });
      _store(p, Entry.constant(1, { uuid: U1 }));
      assert.deepEqual([...p.keys({ include_index: true })], [BASE]);
    });

    t("test_index_survives_reopen", () => {
      const p = path.join(tmp_path(), "re.sqlite");
      const a = new SQLitePool({ file_path: p, uuid: U1 });
      const [base] = _store_versions(a, 3);
      a.close();
      const b = new SQLitePool({ file_path: p, uuid: U1 });
      assert.deepEqual(
        b.index.candidates(base),
        range(3).map((i) => `${base}@evolution=${i}`),
      );
      b.close();
    });

    test("test_write_async_indexes", async () => {
      const e = Entry.variable(1, { uuid: U1 });
      const rec = new Record({ entry: e });
      const blob = pool.transformations !== null && pool.transformations !== undefined ? rec.serialize(pool.transformations) : rec.as_dict;
      await macrotask(async () => {
        await pool.write_async(e.global_id, blob);
      });
      await macrotask(() => {
        assert.deepEqual(pool.index.candidates(BASE), [e.global_id]);
      });
    });

    t("test_search_keys_delegates", () => {
      const [base] = _store_versions(pool, 2);
      assert.deepEqual(pool._search_keys(base, {}), pool.index.candidates(base));
      pool.index_enabled = false;
      assert.equal(pool._search_keys(base, {}), null);
    });

    t("test_candidate_keys_scan", () => {
      const [base] = _store_versions(pool, 2);
      assert.deepEqual(sorted(pool._candidate_keys(base)), [`${base}@evolution=0`, `${base}@evolution=1`]);
    });

    t("test_many_bases", () => {
      const bases = [];
      for (const i of range(10)) {
        const [b] = _store_versions(pool, 2, `many${i}`);
        bases.push(b);
      }
      for (const b of bases) assert.equal(pool.index.latest(b), `${b}@evolution=1`);
    });
  });
}

// ---------------------------------------------------------------------------
// Resolution through central memory (black-box)
// ---------------------------------------------------------------------------

describe("TestRememberResolution", () => {
  /** ``mem_pool`` fixture on top of ``fresh_policy``. */
  const tm = (name, fn) =>
    test(name, () =>
      with_fresh_policy((fresh_policy) => {
        const p = new SQLitePool({ file_path: path.join(tmp_path(), "mem.sqlite") });
        fresh_policy.central.memory.extend(p, { pool_nickname: "ix" });
        try {
          return fn(p, fresh_policy);
        } finally {
          p.close();
        }
      }),
    );

  const _versions = (n) => {
    let e = Entry.variable(0, { nickname: "rr" });
    const versions = [e];
    for (let i = 1; i < n; i++) {
      time.sleep(0.002);
      e = e.evolve(i);
      versions.push(e);
    }
    laila.memorize(versions, { pool_nickname: "ix" }).wait(10);
    return versions;
  };

  tm("test_latest_by_negative_evolution", () => {
    const v = _versions(3);
    const base = v[0].global_id.split("@")[0];
    const out = laila.remember(`${base}@evolution=-1`, { pool_nickname: "ix", persist: false }).result;
    assert.equal(out.global_id, v[2].global_id);
    assert.equal(out.data, 2);
  });

  tm("test_bare_reference_is_latest", () => {
    const v = _versions(3);
    const base = v[0].global_id.split("@")[0];
    const out = laila.remember(base, { pool_nickname: "ix", persist: false }).result;
    assert.equal(out.evolution, 2);
  });

  tm("test_second_latest", () => {
    const v = _versions(3);
    const base = v[0].global_id.split("@")[0];
    const out = laila.remember(`${base}@evolution=-2`, { pool_nickname: "ix", persist: false }).result;
    assert.equal(out.evolution, 1);
  });

  tm("test_exact_evolution", () => {
    const v = _versions(3);
    const out = laila.remember(v[0].global_id, { pool_nickname: "ix", persist: false }).result;
    assert.ok(out.evolution === 0 && out.data === 0);
  });

  tm("test_by_creation_timestamp", () => {
    const v = _versions(3);
    const base = v[0].global_id.split("@")[0];
    const out = laila.remember(`${base}@creation_timestamp=${v[1].creation_timestamp}`, { pool_nickname: "ix", persist: false }).result;
    assert.equal(out.evolution, 1);
  });

  tm("test_nickname_shorthand_with_negative_evolution", () => {
    _versions(2);
    const out = laila.remember("rr@evolution=-1", { pool_nickname: "ix", persist: false }).result;
    assert.equal(out.evolution, 1);
  });

  tm("test_nickname_kwarg_with_evolution", () => {
    _versions(2);
    const out = laila.remember({ nickname: "rr", evolution: 0, pool_nickname: "ix", persist: false }).result;
    assert.equal(out.evolution, 0);
  });

  tm("test_out_of_range_negative_raises", () => {
    const v = _versions(2);
    const base = v[0].global_id.split("@")[0];
    assert.throws(() => laila.remember(`${base}@evolution=-5`, { pool_nickname: "ix", persist: false }).wait(10), E.KeyError);
  });

  tm("test_missing_timestamp_raises", () => {
    const v = _versions(2);
    const base = v[0].global_id.split("@")[0];
    assert.throws(
      () => laila.remember(`${base}@creation_timestamp=1999-01-01T00:00:00.000+00:00`, { pool_nickname: "ix", persist: false }).wait(10),
      E.KeyError,
    );
  });

  tm("test_resolution_without_index_falls_back_to_scan", (mem_pool) => {
    const v = _versions(3);
    const base = v[0].global_id.split("@")[0];
    mem_pool.index_enabled = false;
    const out = laila.remember(`${base}@evolution=-1`, { pool_nickname: "ix", persist: false }).result;
    assert.equal(out.evolution, 2);
  });

  tm("test_stale_index_entry_is_healed", (mem_pool) => {
    const v = _versions(3);
    const base = v[0].global_id.split("@")[0];
    mem_pool._delete(v[2].global_id); // out-of-band delete, index still says 2
    const out = laila.remember(`${base}@evolution=-1`, { pool_nickname: "ix", persist: false }).result;
    assert.equal(out.evolution, 1);
  });

  tm("test_forget_updates_index", (mem_pool) => {
    const v = _versions(3);
    const base = v[0].global_id.split("@")[0];
    laila.forget(v[2].global_id, { pool_nickname: "ix" }).wait(10);
    assert.equal(mem_pool.index.latest(base), v[1].global_id);
    const out = laila.remember(`${base}@evolution=-1`, { pool_nickname: "ix", persist: false }).result;
    assert.equal(out.evolution, 1);
  });
});

// Tear down the lazily-activated default policy so the process exits.
test("zz teardown", () =>
  macrotask(() => {
    laila.terminate();
    fs.rmSync(TMP_ROOT, { recursive: true, force: true });
  }));
