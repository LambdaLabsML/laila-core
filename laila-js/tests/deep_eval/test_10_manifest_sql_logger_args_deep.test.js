/**
 * Port of ``tests/deep_eval/test_10_manifest_sql_logger_args_deep.py``.
 *
 * Deep tests for the Manifest SQL index, the Logger singleton and its pool
 * sink, structured log records, and the ArgReader / ``laila.args`` surface.
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
const time = await import(S + "_compat/time.js");
const json = await import(S + "_compat/pyjson.js");
const logging = await import(S + "_compat/logging.js");
const { repr } = await import(S + "_compat/pyrepr.js");
const { DotMap } = await import(S + "_compat/dotmap.js");
const { AtomicDotMap } = await import(S + "atomic/index.js");
const { Entry } = await import(S + "entry/entry.js");
const LG = await import(S + "logger/index.js");
const { _LAILA_LOGGER_NAME, disable_logging, enable_logging, get_logger, set_log_level } = LG;
const { Logger } = await import(S + "logger/logger.js");
const REC = await import(S + "logger/record.js");
const { build_record, normalize_level, numeric_level } = REC;
const { _coerce_id } = REC;
const { DefaultPool, LAILA_DEFAULT_DIRECTORIES } = await import(S + "macros/defaults.js");
const { Manifest } = await import(S + "policy/central/memory/schema/manifest.js");
const { ArgReader } = await import(S + "utils/args/args.js");
const runtime_module = await import(S + "runtime/index.js");

const { NotImplemented, PyFloat, dict_has, getitem } = T;

const WAIT = 10;
const G = "LAILA:ENTRY:00000000-0000-0000-0000-00000000000";

const TMP_ROOT = fs.mkdtempSync(path.join(os.tmpdir(), "laila_deep10_"));
laila.set_default_directory(TMP_ROOT);

const t = (name, fn) => test(name, () => macrotask(fn));
const tp = (name, fn) => test(name, () => with_fresh_policy(fn));
const tmp_path = () => fs.mkdtempSync(path.join(TMP_ROOT, "tmp_"));
const range = (n) => Array.from({ length: n }, (_, i) => i);
const keyset = (m) => new Set(m.keys());
const write_text = (p, body) => fs.writeFileSync(p, body, "utf8");
/** Python ``1e3 == 1000.0``: compare ``PyFloat`` by value. */
const norm = (v) => (v instanceof PyFloat ? Number(v) : v);

/** Blueprint of dict rows; all values must be strings for Manifest. */
function _bp(rows) {
  const out = {};
  for (const [k, v] of Object.entries(rows)) out[k] = Object.fromEntries(Object.entries(v).map(([kk, vv]) => [kk, String(vv)]));
  return out;
}

/** ``people`` fixture. */
function make_people() {
  return new Manifest({
    blueprint: _bp({
      a: { owner: "alice", age: "30", team: "red" },
      b: { owner: "bob", age: "41", team: "blue" },
      c: { owner: "carol", age: "25", team: "red" },
    }),
  });
}
const with_people = (fn) => {
  const m = make_people();
  try {
    return fn(m);
  } finally {
    m.clear_index();
  }
};
const tpe = (name, fn) => t(name, () => with_people(fn));

// ---------------------------------------------------------------------------
// Manifest.sql -- parsing
// ---------------------------------------------------------------------------

describe("TestSqlParse", () => {
  for (const [q, items, alias, where] of [
    ["SELECT * FROM t", ["*"], "t", null],
    ["select * from t;", ["*"], "t", null],
    ["SELECT a, b FROM tbl WHERE a = 1", ["a", "b"], "tbl", "a = 1"],
    ["SELECT t.a FROM t WHERE t.a == 'x'", ["a"], "t", "t.a = 'x'"],
    ["SELECT `a` FROM `t`", ["a"], "t", null],
    ['SELECT "a" FROM "t"', ["a"], "t", null],
    ["  SELECT *\nFROM t\nWHERE x > 2  ", ["*"], "t", "x > 2"],
  ]) {
    test(`test_parse[${JSON.stringify(q)}]`, () => {
      assert.deepEqual([...Manifest._parse_sql(q)], [items, alias, where]);
    });
  }

  for (const q of [
    "SELECT * FROM t ORDER BY x",
    "SELECT * FROM t LIMIT 1",
    "SELECT * FROM t GROUP BY x",
    "SELECT * FROM t JOIN u ON 1",
    "SELECT * FROM t UNION SELECT * FROM u",
    "SELECT * FROM t WHERE x = 1 HAVING y",
    "DELETE FROM t",
    "INSERT INTO t VALUES (1)",
    "FROM t SELECT *",
    "SELECT * FROM",
    "SELECT * FROM 1t",
    "",
  ]) {
    test(`test_rejects[${JSON.stringify(q)}]`, () => {
      assert.throws(() => Manifest._parse_sql(q), E.ValueError);
    });
  }

  test("test_non_string_rejected", () => {
    assert.throws(() => Manifest._parse_sql(123), E.ValueError);
  });

  test("test_strip_table_prefix", () => {
    assert.equal(Manifest._strip_table_prefix("t.col"), "col");
    assert.equal(Manifest._strip_table_prefix('"t"."col"'), "col");
    assert.equal(Manifest._strip_table_prefix("*"), "*");
    assert.equal(Manifest._strip_table_prefix("`col`"), "col");
  });

  test("test_forbidden_clause_inside_literal_rejected", () => {
    // Documented limitation: the clause scan does not understand quoting.
    assert.throws(() => Manifest._parse_sql("SELECT * FROM t WHERE owner = 'order by'"), E.ValueError);
  });

  test("test_column_named_limit_rejected", () => {
    assert.throws(() => Manifest._parse_sql("SELECT * FROM t WHERE limit = 1"), E.ValueError);
  });

  test("test_validate_ident", () => {
    Manifest._validate_ident("good_name1");
    for (const bad of ["bad name", "1abc", "a-b", "", 'a"b', null]) assert.throws(() => Manifest._validate_ident(bad), E.ValueError);
  });
});

// ---------------------------------------------------------------------------
// Manifest.sql -- execution
// ---------------------------------------------------------------------------

describe("TestSqlQuery", () => {
  tpe("test_where_equals", (people) => {
    assert.deepEqual(people.sql("SELECT * FROM p WHERE owner = 'alice'").data, { a: { owner: "alice", age: "30", team: "red" } });
  });

  tpe("test_double_equals_normalized", (people) => {
    assert.deepEqual(keyset(people.sql("SELECT * FROM p WHERE team == 'red'")), new Set(["a", "c"]));
  });

  tpe("test_no_where_returns_all", (people) => {
    assert.deepEqual(people.sql("SELECT * FROM p").data, people.data);
  });

  tpe("test_result_is_new_manifest", (people) => {
    const r = people.sql("SELECT * FROM p");
    assert.ok(r instanceof Manifest);
    assert.notEqual(r, people);
    assert.notEqual(r.global_id, people.global_id);
  });

  tpe("test_result_is_deep_copy", (people) => {
    const r = people.sql("SELECT * FROM p WHERE owner = 'bob'");
    r.data.b.owner = "mutated";
    assert.equal(people.data.b.owner, "bob");
  });

  tpe("test_sql_does_not_mutate_self", (people) => {
    const before = json.dumps(people.data, { sort_keys: true });
    people.sql("SELECT * FROM p WHERE owner = 'bob'");
    assert.equal(json.dumps(people.data, { sort_keys: true }), before);
  });

  tpe("test_empty_result", (people) => {
    const r = people.sql("SELECT * FROM p WHERE owner = 'nobody'");
    assert.deepEqual(r.data, {});
    assert.equal(r.__len__(), 0);
  });

  tpe("test_string_comparison", (people) => {
    // Values are TEXT: lexical ordering.
    assert.deepEqual(keyset(people.sql("SELECT * FROM p WHERE age > '3'")), new Set(["a", "b"]));
  });

  tpe("test_cast_comparison", (people) => {
    assert.deepEqual(keyset(people.sql("SELECT * FROM p WHERE CAST(age AS INTEGER) > 28")), new Set(["a", "b"]));
  });

  tpe("test_like", (people) => {
    assert.deepEqual(keyset(people.sql("SELECT * FROM p WHERE owner LIKE 'c%'")), new Set(["c"]));
  });

  tpe("test_in", (people) => {
    assert.deepEqual(keyset(people.sql("SELECT * FROM p WHERE owner IN ('alice','carol')")), new Set(["a", "c"]));
  });

  tpe("test_and_or", (people) => {
    assert.deepEqual(keyset(people.sql("SELECT * FROM p WHERE team = 'red' AND age = '25'")), new Set(["c"]));
    assert.deepEqual(keyset(people.sql("SELECT * FROM p WHERE owner = 'bob' OR owner = 'carol'")), new Set(["b", "c"]));
  });

  tpe("test_bareword_rhs_gives_helpful_error", (people) => {
    assert.throws(
      () => people.sql("SELECT * FROM p WHERE owner = alice"),
      (e) => e instanceof E.ValueError && /single/.test(e.message),
    );
  });

  tpe("test_unknown_column_error", (people) => {
    assert.throws(
      () => people.sql("SELECT * FROM p WHERE nosuch = 'x'"),
      (e) => e instanceof E.ValueError && /no such column/.test(e.message),
    );
  });

  tpe("test_select_items_ignored_by_projection", (people) => {
    const r = people.sql("SELECT owner FROM p WHERE owner = 'alice'");
    assert.deepEqual(r.data, { a: { owner: "alice", age: "30", team: "red" } });
  });

  t("test_scalar_rows_use_value_column", () => {
    const m = new Manifest({ blueprint: { x: G + "1", y: G + "2" } });
    try {
      const r = m.sql(`SELECT * FROM t WHERE value = '${G}2'`);
      assert.deepEqual(r.data, { y: G + "2" });
    } finally {
      m.clear_index();
    }
  });

  t("test_mixed_scalar_and_dict_rows", () => {
    const m = new Manifest({ blueprint: { x: G + "1", y: { owner: "bob" } } });
    try {
      assert.deepEqual(keyset(m.sql("SELECT * FROM t WHERE value IS NULL")), new Set(["y"]));
      assert.deepEqual(keyset(m.sql("SELECT * FROM t WHERE owner IS NULL")), new Set(["x"]));
    } finally {
      m.clear_index();
    }
  });

  t("test_null_handling_for_missing_columns", () => {
    const m = new Manifest({ blueprint: _bp({ a: { x: "1" }, b: { y: "2" } }) });
    try {
      assert.deepEqual(keyset(m.sql("SELECT * FROM t WHERE x IS NULL")), new Set(["b"]));
    } finally {
      m.clear_index();
    }
  });

  t("test_list_valued_rows_rejected", () => {
    const m = new Manifest({ blueprint: { a: { tags: ["1", "2"] } } });
    assert.throws(() => m.sql("SELECT * FROM t"), E.TypeError);
  });

  t("test_nested_dict_rows_rejected", () => {
    const m = new Manifest({ blueprint: { a: { meta: { k: "v" } } } });
    assert.throws(() => m.sql("SELECT * FROM t"), E.TypeError);
  });

  test(
    "test_multi_statement_rejected",
    () =>
      with_people((people) => {
        assert.throws(
          () => people.sql("SELECT * FROM p WHERE 1=1; DROP TABLE x"),
          (e) => e instanceof E.ValueError || e instanceof E.SqliteProgrammingError,
        );
      }),
  );

  t("test_empty_manifest_sql", () => {
    const m = new Manifest();
    try {
      assert.deepEqual(m.sql("SELECT * FROM t").data, {});
    } finally {
      m.clear_index();
    }
  });

  t("test_subclass_preserved_by_sql", () => {
    class MyM extends Manifest {}

    const m = new MyM({ blueprint: _bp({ a: { x: "1" } }) });
    try {
      assert.equal(m.sql("SELECT * FROM t").constructor, MyM);
    } finally {
      m.clear_index();
    }
  });

  t("test_sub_manifest_and_add_return_base_manifest", () => {
    class MyM extends Manifest {}

    const m = new MyM({ blueprint: _bp({ a: { x: "1" } }) });
    // Inconsistent with sql(): these two drop the subclass.
    assert.equal(m.sub_manifest(["a"]).constructor, Manifest);
    assert.equal(m.__add__(new MyM({ blueprint: _bp({ b: { x: "2" } }) })).constructor, Manifest);
  });

  tpe("test_concurrent_queries", (people) => {
    people.build_index();
    const out = [];
    const lock = new TH.Lock();
    const q = (name) => {
      const r = people.sql(`SELECT * FROM p WHERE owner = '${name}'`);
      TH.with_lock(lock, () => out.push([name, { ...r.data }]));
    };
    const ts = [...["alice", "bob", "carol"], ...["alice", "bob", "carol"], ...["alice", "bob", "carol"], ...["alice", "bob", "carol"]].map(
      (n) => new TH.Thread({ target: q, args: [n] }),
    );
    for (const th of ts) th.start();
    for (const th of ts) th.join(WAIT);
    assert.equal(out.length, 12);
    const bad = out.filter(([n, d]) => Object.keys(d).length !== 1 || Object.values(d)[0].owner !== n);
    assert.deepEqual(bad, []);
  });

  tpe("test_shared_index_connection_is_thread_safe", (people) => {
    people.build_index();
    const state = people._sql_state;
    const bad = [];
    const lock = new TH.Lock();
    const q = (name) => {
      for (let i = 0; i < 200; i++) {
        const rows = state.conn.execute(`SELECT * FROM "${state.table_name}" WHERE owner = '${name}'`).fetchall();
        if (rows.length !== 1) TH.with_lock(lock, () => bad.push([name, rows]));
      }
    };
    const ts = [...["alice", "bob", "carol"], ...["alice", "bob", "carol"], ...["alice", "bob", "carol"], ...["alice", "bob", "carol"]].map(
      (n) => new TH.Thread({ target: q, args: [n] }),
    );
    for (const th of ts) th.start();
    for (const th of ts) th.join(WAIT);
    assert.deepEqual(bad, [], repr(bad.slice(0, 5)));
  });

  t("test_many_rows", () => {
    const m = new Manifest({ blueprint: _bp(Object.fromEntries(range(500).map((i) => [`k${i}`, { n: String(i).padStart(4, "0") }]))) });
    try {
      assert.equal(m.sql("SELECT * FROM t WHERE n >= '0490'").__len__(), 10);
    } finally {
      m.clear_index();
    }
  });
});

// ---------------------------------------------------------------------------
// Manifest index lifecycle
// ---------------------------------------------------------------------------

describe("TestSqlIndexLifecycle", () => {
  tpe("test_lazy_build", (people) => {
    assert.equal(people._sql_state, null);
    people.sql("SELECT * FROM p");
    assert.notEqual(people._sql_state, null);
    assert.equal(people._sql_state.stale, false);
  });

  tpe("test_build_index_idempotent", (people) => {
    people.build_index();
    const conn = people._sql_state.conn;
    people.build_index();
    assert.equal(people._sql_state.conn, conn);
  });

  tpe("test_columns_sorted", (people) => {
    people.build_index();
    assert.deepEqual([...people._sql_state.columns], ["age", "owner", "team"]);
  });

  tpe("test_row_keys_order", (people) => {
    people.build_index();
    assert.deepEqual([...people._sql_state.row_keys], ["a", "b", "c"]);
  });

  tpe("test_invalidate_marks_stale", (people) => {
    people.build_index();
    people.invalidate_index();
    assert.equal(people._sql_state.stale, true);
    people.sql("SELECT * FROM p");
    assert.equal(people._sql_state.stale, false);
  });

  tpe("test_invalidate_without_index_noop", (people) => {
    people.invalidate_index();
    assert.equal(people._sql_state, null);
  });

  tpe("test_extend_invalidates", (people) => {
    people.build_index();
    people.extend(new Manifest({ blueprint: _bp({ d: { owner: "dan", age: "50", team: "blue" } }) }));
    assert.equal(people._sql_state.stale, true);
    assert.deepEqual(keyset(people.sql("SELECT * FROM p WHERE team = 'blue'")), new Set(["b", "d"]));
  });

  tpe("test_iadd_invalidates", (people) => {
    people.build_index();
    people.__iadd__(new Manifest({ blueprint: _bp({ d: { owner: "dan", age: "50", team: "blue" } }) }));
    assert.deepEqual(keyset(people.sql("SELECT * FROM p WHERE owner = 'dan'")), new Set(["d"]));
  });

  tpe("test_rebuild_widens_columns", (people) => {
    people.build_index();
    people.extend(new Manifest({ blueprint: _bp({ e: { owner: "eve", city: "oslo" } }) }));
    assert.deepEqual(keyset(people.sql("SELECT * FROM p WHERE city = 'oslo'")), new Set(["e"]));
    assert.ok(people._sql_state.columns.includes("city"));
  });

  tpe("test_rebuild_widen_false_raises", (people) => {
    people.build_index();
    people.extend(new Manifest({ blueprint: _bp({ e: { owner: "eve", city: "oslo" } }) }));
    assert.throws(() => people.build_index({ widen: false }), E.ValueError);
  });

  tpe("test_rebuild_preserves_indexes", (people) => {
    people.build_index({ on: ["owner"] });
    people.invalidate_index();
    people.sql("SELECT * FROM p");
    assert.ok(Manifest._existing_indexes(people._sql_state.conn).has("idx_" + people._sql_state.table_name + "_owner"));
  });

  tpe("test_build_index_on_and_composite", (people) => {
    people.build_index({ on: ["owner"], composite: [["owner", "team"]] });
    const indexed = [...people._sql_state.indexed];
    assert.equal(indexed.length, 2);
    assert.ok(indexed.includes("owner"));
    assert.ok(indexed.some((x) => Array.isArray(x) && x.length === 2 && x[0] === "owner" && x[1] === "team"));
  });

  tpe("test_build_index_bad_identifier", (people) => {
    assert.throws(() => people.build_index({ on: ["bad col"] }), E.ValueError);
  });

  tpe("test_clear_index_closes_and_removes", (people) => {
    people.build_index();
    const p = people._sql_state.db_path;
    const conn = people._sql_state.conn;
    assert.ok(fs.existsSync(p));
    people.clear_index();
    assert.equal(people._sql_state, null);
    assert.ok(!fs.existsSync(p));
    // sqlite3.ProgrammingError ("Cannot operate on a closed database.")
    assert.throws(
      () => conn.execute("SELECT 1"),
      (e) => e?.code === "ERR_INVALID_STATE",
    );
  });

  tpe("test_clear_index_noop_when_absent", (people) => {
    people.clear_index();
  });

  tpe("test_rebuild_after_clear", (people) => {
    people.sql("SELECT * FROM p");
    people.clear_index();
    assert.deepEqual(keyset(people.sql("SELECT * FROM p WHERE team = 'red'")), new Set(["a", "c"]));
  });

  tpe("test_default_path_under_indices_dir", (people) => {
    people.build_index();
    assert.ok(people._sql_state.db_path.startsWith(LAILA_DEFAULT_DIRECTORIES.indices));
    assert.ok(people._sql_state.db_path.includes(people.uuid));
    assert.equal(people._sql_state.is_persistent, false);
  });

  tpe("test_finalizer_registered_for_temp_index", (people) => {
    people.build_index();
    assert.notEqual(people._sql_state, null);
    assert.notEqual(people._sql_finalizer, null);
    assert.ok(people._sql_finalizer.alive);
  });

  t("test_persist_path_kept_after_clear", () => {
    const p = path.join(tmp_path(), "sub", "idx.db");
    const m = new Manifest({ blueprint: _bp({ a: { owner: "alice" } }) });
    m.build_index({ persist: p });
    assert.equal(m._sql_state.is_persistent, true);
    assert.equal(m._sql_finalizer, null);
    m.clear_index();
    assert.ok(fs.existsSync(p));
  });

  t("test_persist_remove_persisted", () => {
    const p = path.join(tmp_path(), "idx.db");
    const m = new Manifest({ blueprint: _bp({ a: { owner: "alice" } }) });
    m.build_index({ persist: p });
    m.clear_index({ remove_persisted: true });
    assert.ok(!fs.existsSync(p));
  });

  t("test_persist_reattach_same_blueprint", () => {
    const p = path.join(tmp_path(), "idx.db");
    const m = new Manifest({ blueprint: _bp({ a: { owner: "alice" }, b: { owner: "bob" } }) });
    m.build_index({ persist: p });
    m.clear_index();
    const m2 = new Manifest({ blueprint: _bp({ a: { owner: "alice" }, b: { owner: "bob" } }) });
    m2.build_index({ persist: p });
    try {
      assert.deepEqual(keyset(m2.sql("SELECT * FROM t WHERE owner = 'bob'")), new Set(["b"]));
    } finally {
      m2.clear_index({ remove_persisted: true });
    }
  });

  t("test_persist_reattach_detects_changed_rows", () => {
    const p = path.join(tmp_path(), "idx.db");
    const m = new Manifest({ blueprint: _bp({ a: { owner: "alice" }, b: { owner: "bob" } }) });
    m.build_index({ persist: p });
    m.clear_index();
    const m2 = new Manifest({ blueprint: _bp({ a: { owner: "zed" }, b: { owner: "yan" } }) });
    m2.build_index({ persist: p });
    try {
      assert.deepEqual(m2.sql("SELECT * FROM t WHERE owner = 'alice'").data, {});
      assert.deepEqual(keyset(m2.sql("SELECT * FROM t WHERE owner = 'zed'")), new Set(["a"]));
    } finally {
      m2.clear_index({ remove_persisted: true });
    }
  });

  t("test_same_nickname_manifests_have_independent_indexes", () => {
    const n1 = new Manifest({ blueprint: _bp({ a: { owner: "alice" }, b: { owner: "bob" } }), nickname: "deep-eval-shared-nick" });
    const n2 = new Manifest({ blueprint: _bp({ a: { owner: "zed" }, b: { owner: "yan" } }), nickname: "deep-eval-shared-nick" });
    try {
      n1.build_index();
      assert.deepEqual(n2.sql("SELECT * FROM t WHERE owner = 'alice'").data, {});
    } finally {
      n1.clear_index();
      n2.clear_index();
    }
  });

  t("test_same_nickname_manifests_share_dir_not_file", () => {
    const n1 = new Manifest({ blueprint: _bp({ a: { owner: "alice" } }), nickname: "deep-eval-shared-nick-2" });
    const n2 = new Manifest({ blueprint: _bp({ a: { owner: "zed" } }), nickname: "deep-eval-shared-nick-2" });
    let p1, p2;
    try {
      [p1, p2] = [n1._index_db_path(), n2._index_db_path()];
      assert.equal(path.dirname(p1), path.dirname(p2));
      assert.notEqual(p1, p2);
    } finally {
      n1.clear_index();
      n2.clear_index();
      Manifest._sql_unlink_db(p1);
      Manifest._sql_unlink_db(p2);
    }
  });

  tpe("test_extend_marks_locally_modified", (people) => {
    people.extend(new Manifest({ blueprint: _bp({ z: { owner: "zed" } }) }));
    assert.equal(people.locally_modified, true);
  });
});

// ---------------------------------------------------------------------------
// Manifest mapping surface (complements suite 5)
// ---------------------------------------------------------------------------

describe("TestManifestMapping", () => {
  tpe("test_keys_values_items", (people) => {
    assert.deepEqual([...people.keys()], ["a", "b", "c"]);
    assert.equal([...people.values()].length, 3);
    assert.equal(Object.fromEntries(people.items()).a.owner, "alice");
  });

  t("test_empty_mapping", () => {
    const m = new Manifest();
    assert.ok([...m.keys()].length === 0 && [...m.values()].length === 0 && [...m.items()].length === 0);
    assert.equal(m.__len__(), 0);
    assert.throws(() => m.__getitem__("x"), E.KeyError);
    assert.throws(() => m.sub_manifest(["x"]), E.RuntimeError);
  });

  tpe("test_iter_yields_all_leaf_strings", (people) => {
    // Every leaf string is yielded, even when it is not a global id.
    assert.ok([...people].includes("alice"));
    assert.ok(people.__contains__("alice"));
    assert.ok(!people.__contains__(123));
  });

  tpe("test_repr_counts_leaves", (people) => {
    assert.ok(repr(people).endsWith("entries=9)"));
    assert.equal(T.str(people), people.global_id);
  });

  tpe("test_sub_manifest", (people) => {
    const s = people.sub_manifest(["a", "c"]);
    assert.deepEqual(keyset(s), new Set(["a", "c"]));
    s.data.a.owner = "x";
    assert.equal(people.data.a.owner, "alice");
  });

  tpe("test_sub_manifest_missing", (people) => {
    assert.throws(() => people.sub_manifest(["zz"]), E.KeyError);
  });

  tpe("test_extend_duplicate_keys", (people) => {
    assert.throws(() => people.extend(new Manifest({ blueprint: _bp({ a: { owner: "dup" } }) })), E.KeyError);
  });

  tpe("test_extend_overwrite", (people) => {
    people.extend(new Manifest({ blueprint: _bp({ a: { owner: "dup" } }) }), { overwrite: true });
    assert.deepEqual(people.__getitem__("a"), { owner: "dup" });
  });

  tpe("test_extend_type_error", (people) => {
    assert.throws(() => people.extend({ a: 1 }), E.TypeError);
  });

  tpe("test_extend_empty_other_noop", (people) => {
    const before = { ...people.data };
    people.extend(new Manifest());
    assert.deepEqual(people.data, before);
  });

  t("test_extend_into_empty", () => {
    const m = new Manifest();
    m.extend(new Manifest({ blueprint: { x: G + "1" } }));
    assert.equal(m.__getitem__("x"), G + "1");
  });

  tpe("test_add_new_manifest", (people) => {
    const other = new Manifest({ blueprint: _bp({ d: { owner: "dan" } }) });
    const r = people.__add__(other);
    assert.deepEqual(keyset(r), new Set(["a", "b", "c", "d"]));
    assert.deepEqual(keyset(people), new Set(["a", "b", "c"]));
  });

  tpe("test_add_duplicate_raises", (people) => {
    assert.throws(() => people.__add__(new Manifest({ blueprint: _bp({ a: { owner: "dup" } }) })), E.KeyError);
  });

  tpe("test_add_non_manifest", (people) => {
    // ``people + 1`` raises TypeError in Python because ``__add__`` answers
    // ``NotImplemented``; JS has no operator dispatch, so the observable
    // contract is that sentinel.
    assert.equal(people.__add__(1), NotImplemented);
  });

  t("test_add_two_empty", () => {
    const r = new Manifest().__add__(new Manifest());
    assert.equal(r.__len__(), 0);
  });

  tpe("test_iadd_returns_self", (people) => {
    const before = people;
    const after = people.__iadd__(new Manifest({ blueprint: _bp({ d: { owner: "dan" } }) }));
    assert.equal(after, before);
  });

  t("test_floating_leaves", () => {
    const m = new Manifest({
      blueprint: { x: G + "1@evolution=-1", y: G + "2@creation_timestamp=123", z: G + "3", w: G + "4@evolution=2" },
    });
    assert.deepEqual(m.floating_leaves(), [G + "1@evolution=-1", G + "2@creation_timestamp=123"]);
  });

  t("test_floating_leaves_ignores_malformed", () => {
    const m = new Manifest({ blueprint: { x: "not-a-gid" } });
    assert.deepEqual(m.floating_leaves(), []);
  });

  t("test_blueprint_validation", () => {
    assert.throws(() => new Manifest({ blueprint: { a: 1 } }), E.ValueError);
    assert.throws(() => new Manifest({ blueprint: { a: [1, 2] } }), E.ValueError);
  });

  t("test_memorize_empty_raises", () => {
    assert.throws(() => new Manifest().memorize(), E.RuntimeError);
    assert.throws(() => new Manifest().remember(), E.RuntimeError);
    assert.throws(() => new Manifest().forget(), E.RuntimeError);
  });
});

// ---------------------------------------------------------------------------
// Log records
// ---------------------------------------------------------------------------

describe("TestLogRecord", () => {
  for (const [level, expected] of [
    ["debug", "DEBUG"],
    ["Info", "INFO"],
    ["WARNING", "WARNING"],
    ["error", "ERROR"],
    ["critical", "CRITICAL"],
    [10, "DEBUG"],
    [20, "INFO"],
    [30, "WARNING"],
    [40, "ERROR"],
    [50, "CRITICAL"],
    ["bogus", "INFO"],
    [99, "INFO"],
    [null, "INFO"],
    [3.5, "INFO"],
  ]) {
    test(`test_normalize_level[${JSON.stringify(level)}-${expected}]`, () => {
      assert.equal(normalize_level(level), expected);
    });
  }

  test("test_numeric_level", () => {
    assert.equal(numeric_level("ERROR"), logging.ERROR);
    assert.equal(numeric_level(10), 10);
    assert.equal(numeric_level("bogus"), logging.INFO);
  });

  test("test_warn_alias_not_recognised", () => {
    // "WARN" (stdlib alias) silently degrades to INFO.
    assert.equal(normalize_level("WARN"), "INFO");
  });

  test(
    "test_coerce_id",
    () =>
      macrotask(() => {
        assert.equal(_coerce_id(null), null);
        assert.equal(_coerce_id("x"), "x");
        const e = Entry.constant(1);
        assert.equal(_coerce_id(e), e.global_id);
        assert.equal(_coerce_id(5), "5");
      }),
  );

  test("test_build_record_minimal", () => {
    const r = build_record("evt");
    assert.deepEqual(new Set(Object.keys(r)), new Set(["ts", "ts_unix", "level", "event", "extra"]));
    assert.equal(r.level, "INFO");
    assert.equal(r.event, "evt");
    assert.deepEqual(r.extra, {});
    assert.ok(r.ts.endsWith("Z"));
    assert.ok(Math.abs(Number(r.ts_unix) - time.time()) < 5);
  });

  test("test_build_record_omits_none", () => {
    const r = build_record("evt", { policy_id: null, message: null });
    assert.ok(!("policy_id" in r) && !("message" in r));
  });

  t("test_build_record_ids_coerced", () => {
    const e = Entry.constant(1);
    const r = build_record("evt", { entry_id: e, pool_id: "p", future_id: e, child_future_ids: [e, "s"], child_results: [e] });
    assert.equal(r.entry_id, e.global_id);
    assert.deepEqual(r.child_future_ids, [e.global_id, "s"]);
    assert.deepEqual(r.child_results, [e.global_id]);
  });

  test("test_build_record_nicknames_str", () => {
    const r = build_record("evt", { pool_nickname: 5, entry_nickname: null, purpose: 7 });
    assert.ok(r.pool_nickname === "5" && r.purpose === "7");
    assert.ok(!("entry_nickname" in r));
  });

  test("test_build_record_status", () => {
    const r = build_record("evt", { status: "finished", prev_status: "running" });
    assert.ok(r.status === "finished" && r.prev_status === "running");
  });

  test("test_build_record_extra_copied", () => {
    const extra = { a: 1 };
    const r = build_record("evt", { extra });
    extra.a = 2;
    assert.deepEqual(r.extra, { a: 1 });
  });

  test("test_build_record_level_normalized", () => {
    assert.equal(build_record("evt", { level: "error" }).level, "ERROR");
  });

  t("test_build_record_json_serialisable", () => {
    json.dumps(build_record("evt", { policy_id: Entry.constant(1), extra: { x: [1, 2] } }));
  });
});

// ---------------------------------------------------------------------------
// Logger singleton & sinks
// ---------------------------------------------------------------------------

/** ``logger`` fixture. */
function with_logger(fn) {
  Logger.reset_singleton();
  const lg = get_logger();
  const teardown = () => {
    try {
      lg.stop();
    } finally {
      Logger.reset_singleton();
    }
  };
  let out;
  try {
    out = fn(lg);
  } catch (e) {
    teardown();
    throw e;
  }
  if (out !== null && out !== undefined && typeof out.then === "function") return out.then(teardown, (e) => (teardown(), Promise.reject(e))).then(() => out);
  teardown();
  return out;
}

/** ``capture`` fixture: a DEBUG stream handler on the ``laila`` root. */
function with_capture(fn) {
  const root = logging.getLogger(_LAILA_LOGGER_NAME);
  const buf = { value: "", getvalue: () => buf.value };
  const h = new logging.StreamHandler({ write: (s) => void (buf.value += s) });
  h.setLevel(logging.DEBUG);
  const prev_level = root.level;
  root.addHandler(h);
  root.setLevel(logging.DEBUG);
  const teardown = () => {
    root.removeHandler(h);
    root.setLevel(prev_level);
  };
  let out;
  try {
    out = fn(buf);
  } catch (e) {
    teardown();
    throw e;
  }
  if (out !== null && out !== undefined && typeof out.then === "function") return out.then(teardown, (e) => (teardown(), Promise.reject(e))).then(() => out);
  teardown();
  return out;
}

const tl = (name, fn) => t(name, () => with_logger(fn));
const tlc = (name, fn) => t(name, () => with_logger((lg) => with_capture((cap) => fn(lg, cap))));
const tlcp = (name, fn) => test(name, () => with_logger((lg) => with_capture((cap) => with_fresh_policy((fp) => fn(lg, cap, fp)))));
const tlp = (name, fn) => test(name, () => with_logger((lg) => with_fresh_policy((fp) => fn(lg, fp))));

describe("TestLoggerSingleton", () => {
  tl("test_get_logger_is_singleton", (logger) => {
    assert.equal(get_logger(), logger);
    assert.equal(new Logger(), logger);
  });

  tl("test_default_disabled_and_silent", (logger) => {
    assert.equal(logger.enabled, false);
    assert.equal(logger.display, false);
    assert.ok(logger.pool_nickname === null && logger.pool_id === null);
    assert.equal(logger.level, "DEBUG");
  });

  tl("test_scope", (logger) => {
    assert.ok(logger.global_id.startsWith("LAILA:LOGGER:"));
  });

  tl("test_constructor_updates_existing", (logger) => {
    const l2 = new Logger({ level: "ERROR", display: false });
    assert.equal(l2, logger);
    assert.equal(logger.level, "ERROR");
  });

  tl("test_constructor_ignores_unknown_fields", (logger) => {
    new Logger({ bogus: 1 });
    assert.ok(!("bogus" in logger));
  });

  tl("test_reset_singleton", (logger) => {
    logger.start();
    Logger.reset_singleton();
    assert.equal(Object.prototype.hasOwnProperty.call(Logger, "_singleton") ? Logger._singleton : null, null);
    assert.equal(logger.enabled, false);
    const fresh = get_logger();
    assert.notEqual(fresh, logger);
    fresh.stop();
  });

  t("test_reset_when_absent", () => {
    Logger.reset_singleton();
    Logger.reset_singleton();
  });

  tl("test_start_forces_display_without_pool", (logger) => {
    logger.start();
    assert.equal(logger.display, true);
    assert.equal(logger.enabled, true);
    assert.equal(logger._installed_handlers.length, 1);
  });

  tl("test_start_idempotent_handlers", (logger) => {
    logger.start();
    logger.start();
    logger.start();
    assert.equal(logger._installed_handlers.length, 1);
  });

  tl("test_stop_removes_handlers", (logger) => {
    logger.start();
    logger.stop();
    assert.equal(logger.enabled, false);
    assert.deepEqual([...logger._installed_handlers], []);
  });

  tl("test_set_level", (logger) => {
    logger.start();
    logger.set_level("warning");
    assert.equal(logger.level, "WARNING");
    assert.equal(logging.getLogger(_LAILA_LOGGER_NAME).level, logging.WARNING);
    assert.ok(logger._installed_handlers.every((h) => h.level === logging.WARNING));
  });

  tl("test_set_level_numeric", (logger) => {
    logger.set_level(40);
    assert.equal(logger.level, "ERROR");
  });

  tl("test_set_log_level_helper", (logger) => {
    set_log_level("CRITICAL");
    assert.equal(logger.level, "CRITICAL");
  });

  tl("test_enable_logging_helper", (logger) => {
    const lg = enable_logging("INFO", { display: true, capture_traceback: true });
    assert.equal(lg, logger);
    assert.ok(lg.enabled && lg.display && lg.capture_traceback);
    assert.equal(lg.level, "INFO");
  });

  tl("test_enable_logging_bogus_level_degrades", (logger) => {
    enable_logging("bogus");
    assert.equal(numeric_level(logger.level), logging.INFO);
  });

  tl("test_disable_logging_helper", (logger) => {
    enable_logging();
    disable_logging();
    assert.equal(logger.enabled, false);
  });

  t("test_disable_logging_without_singleton", () => {
    Logger.reset_singleton();
    disable_logging();
  });

  tl("test_enable_logging_pool_sticky", (logger) => {
    enable_logging("DEBUG", { pool_nickname: "deep-eval-nope" });
    enable_logging();
    // Previous pool configuration survives a reconfigure that omits it.
    assert.equal(logger.pool_nickname, "deep-eval-nope");
    assert.equal(logger.display, false);
  });

  t("test_enabled_at_construction_starts", () => {
    Logger.reset_singleton();
    const lg = new Logger({ enabled: true });
    try {
      assert.ok(lg.enabled && lg.display);
    } finally {
      Logger.reset_singleton();
    }
  });

  tl("test_null_handler_installed", () => {
    const root = logging.getLogger(_LAILA_LOGGER_NAME);
    assert.ok(root.handlers.some((h) => h instanceof logging.NullHandler));
  });
});

describe("TestLoggerEmission", () => {
  tlc("test_emit_noop_when_disabled", (logger, capture) => {
    logger.info("hidden");
    assert.equal(capture.getvalue(), "");
  });

  tlc("test_emit_writes_stdlib", (logger, capture) => {
    logger.display = false;
    logger.enabled = true;
    logger.info("hello-deep");
    assert.ok(capture.getvalue().includes("hello-deep"));
    assert.ok(capture.getvalue().includes("'event': 'log'"));
  });

  tlc("test_level_filter", (logger, capture) => {
    logger.enabled = true;
    logger.level = "WARNING";
    logger.info("nope");
    logger.warning("yes");
    const out = capture.getvalue();
    assert.ok(!out.includes("nope") && out.includes("yes"));
  });

  tlc("test_all_levels", (logger, capture) => {
    logger.enabled = true;
    for (const name of ["debug", "info", "warning", "error", "critical"]) logger[name](`m-${name}`);
    const out = capture.getvalue();
    assert.ok(["debug", "info", "warning", "error", "critical"].every((n) => out.includes(`m-${n}`)));
  });

  tl("test_emit_adds_logger_id", (logger) => {
    logger.enabled = true;
    const rec = build_record("x");
    logger.emit(rec);
    assert.equal(rec.logger_id, logger.global_id);
  });

  tl("test_emit_preserves_existing_logger_id", (logger) => {
    logger.enabled = true;
    const rec = build_record("x", { logger_id: "custom" });
    logger.emit(rec);
    assert.equal(rec.logger_id, "custom");
  });

  tlc("test_kwargs_forwarded", (logger, capture) => {
    logger.enabled = true;
    logger.info("m", { extra: { k: "v-deep" }, entry_id: "E1" });
    assert.ok(capture.getvalue().includes("v-deep") && capture.getvalue().includes("E1"));
  });

  tlc("test_recursion_guard", (logger, capture) => {
    logger.enabled = true;
    logger._in_sink.active = true;
    try {
      logger.info("guarded");
    } finally {
      logger._in_sink.active = false;
    }
    assert.ok(!capture.getvalue().includes("guarded"));
  });

  tlc("test_structured_emitters_noop_when_disabled", (logger, capture) => {
    logger.record_memorize({ entries: [Entry.constant(1)], pool: null, policy: null });
    logger.record_remember({ entry_ids: "x", pool: null, policy: null });
    logger.record_forget({ entry_ids: ["x"], pool: null, policy: null });
    logger.record_future_created(null);
    logger.record_future_transition(null, "finished");
    logger.record_group_future_created(null);
    assert.equal(capture.getvalue(), "");
  });

  tlcp("test_record_memorize_single_and_list", (logger, capture, fresh_policy) => {
    logger.enabled = true;
    const e = Entry.constant(1);
    logger.record_memorize({ entries: e, pool: fresh_policy.central.memory.alpha_pool, policy: fresh_policy });
    logger.record_memorize({ entries: [e, e], pool: null, policy: null });
    assert.equal(capture.getvalue().split("memory.memorize").length - 1, 3);
  });

  tlc("test_record_remember_accepts_objects", (logger, capture) => {
    logger.enabled = true;
    const e = Entry.constant(1);
    logger.record_remember({ entry_ids: [e, "gid"], pool: null, policy: null });
    const out = capture.getvalue();
    assert.ok(out.includes(e.global_id) && out.includes("'entry_id': 'gid'"));
  });

  tlc("test_record_forget", (logger, capture) => {
    logger.enabled = true;
    logger.record_forget({ entry_ids: new Set(["a"]), pool: null, policy: null });
    assert.ok(capture.getvalue().includes("memory.forget"));
  });

  tlp("test_pool_nickname_reverse_lookup", (logger, fresh_policy) => {
    const pool = new DefaultPool();
    fresh_policy.central.memory.extend(pool, { pool_nickname: "deep-nick" });
    assert.equal(logger._pool_nickname_of(pool, fresh_policy), "deep-nick");
    assert.equal(logger._pool_nickname_of(new DefaultPool(), fresh_policy), null);
    assert.equal(logger._pool_nickname_of(null, fresh_policy), null);
    assert.equal(logger._pool_nickname_of(pool, null), null);
  });

  tlcp("test_future_lifecycle_records", (logger, capture, fresh_policy) => {
    logger.enabled = true;
    const f = fresh_policy.central.command.submit([() => 1]);
    f.wait(WAIT);
    time.sleep(0.05);
    const out = capture.getvalue();
    assert.ok(out.includes("future.created"));
    assert.ok(out.includes("'status': 'running'"));
    assert.ok(out.includes("'status': 'finished'"));
    assert.ok(out.includes(f.global_id));
  });

  tlcp("test_group_future_record", (logger, capture, fresh_policy) => {
    logger.enabled = true;
    const g = fresh_policy.central.command.submit([() => 1, () => 2]);
    g.wait(WAIT);
    assert.ok(capture.getvalue().includes("future.group_created"));
    assert.ok(capture.getvalue().includes("group future created with 2 children"));
  });

  tlcp("test_error_transition_is_error_level", (logger, capture, fresh_policy) => {
    logger.enabled = true;
    const boom = () => {
      throw new E.ValueError("bang-deep");
    };
    const f = fresh_policy.central.command.submit([boom]);
    assert.throws(() => f.wait(WAIT), E.ValueError);
    time.sleep(0.05);
    const out = capture.getvalue();
    assert.ok(out.includes("'exc_type': 'ValueError'"));
    assert.ok(out.includes("bang-deep"));
    assert.ok(out.includes("'level': 'ERROR'"));
    assert.ok(!out.includes("Traceback"));
    assert.ok(!out.includes("'traceback'"));
  });

  tlcp("test_capture_traceback", (logger, capture, fresh_policy) => {
    logger.enabled = true;
    logger.capture_traceback = true;
    const boom = () => {
      throw new E.ValueError("bang-tb");
    };
    const f = fresh_policy.central.command.submit([boom]);
    assert.throws(() => f.wait(WAIT), E.ValueError);
    time.sleep(0.05);
    // ``"Traceback" in out``: the CPython traceback header; the V8 analogue
    // recorded under ``traceback`` is the stack-frame listing (``at ...``).
    const out = capture.getvalue();
    assert.ok(out.includes("'traceback'"));
    assert.ok(/ValueError: bang-tb[\s\S]*\bat /.test(out));
  });

  tlcp("test_memorize_remember_records", (logger, capture) => {
    logger.enabled = true;
    const e = Entry.constant(3);
    laila.memorize(e).wait(WAIT);
    laila.remember(e.global_id).wait(WAIT);
    const out = capture.getvalue();
    assert.ok(out.includes("memory.memorize") && out.includes("memory.remember"));
    assert.ok(out.includes(e.global_id));
  });

  tlcp("test_forget_record", (logger, capture) => {
    logger.enabled = true;
    const e = Entry.constant(3);
    laila.memorize(e).wait(WAIT);
    laila.forget(e.global_id).wait(WAIT);
    assert.ok(capture.getvalue().includes("memory.forget"));
  });

  tlcp("test_result_id_in_transition", (logger, capture, fresh_policy) => {
    logger.enabled = true;
    const f = fresh_policy.central.command.submit([() => 5]);
    f.wait(WAIT);
    time.sleep(0.05);
    assert.ok(capture.getvalue().includes(f.result_global_id));
  });
});

describe("TestLoggerPoolSink", () => {
  /** ``sink`` fixture (``fresh_policy`` + ``logger``). */
  const ts = (name, fn) =>
    test(name, () =>
      with_fresh_policy((fresh_policy) =>
        with_logger((logger) => {
          const pool = new DefaultPool();
          fresh_policy.central.memory.extend(pool, { pool_nickname: "deep-logpool" });
          enable_logging("INFO", { pool_nickname: "deep-logpool" });
          return fn(pool, logger, fresh_policy);
        }),
      ),
    );
  const tsc = (name, fn) =>
    test(name, () =>
      with_fresh_policy((fresh_policy) =>
        with_logger((logger) =>
          with_capture((capture) => {
            const pool = new DefaultPool();
            fresh_policy.central.memory.extend(pool, { pool_nickname: "deep-logpool" });
            enable_logging("INFO", { pool_nickname: "deep-logpool" });
            return fn(pool, logger, capture);
          }),
        ),
      ),
    );

  ts("test_display_not_forced_with_pool", (_sink, logger) => {
    assert.equal(logger.display, false);
    assert.deepEqual([...logger._installed_handlers], []);
  });

  ts("test_record_lands_in_pool", (sink, logger) => {
    logger.info("to-pool");
    const keys = [...sink.keys()];
    assert.equal(keys.length, 1);
    assert.equal(logger._last_pool_sink_error, null);
  });

  ts("test_record_round_trips", (sink, logger) => {
    logger.info("to-pool", { extra: { n: 1 } });
    const gid = [...sink.keys()][0];
    const e = laila.remember(gid, { dst_pool: "deep-logpool" }).wait(WAIT);
    assert.equal(e.data.message, "to-pool");
    assert.deepEqual(e.data.extra, { n: 1 });
    assert.equal(e.data.event, "log");
  });

  ts("test_level_filter_applies_to_pool", (sink, logger) => {
    logger.debug("dropped");
    assert.deepEqual([...sink.keys()], []);
  });

  tlp("test_pool_id_sink", (logger, fresh_policy) => {
    const pool = new DefaultPool();
    fresh_policy.central.memory.extend(pool);
    enable_logging("INFO", { pool_id: pool.global_id });
    logger.info("by-id");
    assert.equal([...pool.keys()].length, 1);
  });

  tl("test_missing_pool_records_error", (logger) => {
    enable_logging("INFO", { pool_nickname: "deep-eval-missing" });
    logger.info("x");
    assert.ok(logger._last_pool_sink_error.includes("not found"));
  });

  ts("test_sink_bypasses_index", async (sink, logger) => {
    const { is_index_key } = await import(S + "data/schema/pool_index.js");
    logger.info("a");
    logger.info("b");
    assert.ok(![...sink.keys()].some((k) => is_index_key(k)));
  });

  ts("test_operations_generate_records", (sink) => {
    const e = Entry.constant(1);
    laila.memorize(e).wait(WAIT);
    time.sleep(0.1);
    const events = new Set();
    for (const k of [...sink.keys()]) events.add(laila.remember(k, { dst_pool: "deep-logpool" }).wait(WAIT).data.event);
    for (const ev of ["memory.memorize", "future.created", "future.status"]) assert.ok(events.has(ev), ev);
  });

  ts("test_remember_from_log_pool_does_not_recurse_forever", (sink, logger) => {
    logger.info("seed");
    const gid = [...sink.keys()][0];
    laila.remember(gid, { dst_pool: "deep-logpool" }).wait(WAIT);
    time.sleep(0.1);
    // Bounded amplification: a handful of records, not an explosion.
    assert.ok([...sink.keys()].length < 40);
  });

  tsc("test_both_sinks", (sink, logger, capture) => {
    logger.display = true;
    logger.info("dual");
    assert.ok(capture.getvalue().includes("dual"));
    assert.equal([...sink.keys()].length, 1);
  });
});

// ---------------------------------------------------------------------------
// ArgReader
// ---------------------------------------------------------------------------

describe("TestCoerceScalar", () => {
  for (const [raw, expected] of [
    ["true", true],
    ["True ", true],
    ["FALSE", false],
    ["none", null],
    ["Null", null],
    ["42", 42],
    ["-7", -7],
    ["+7", 7],
    ["3.5", 3.5],
    ["1e3", 1000.0],
    ["[1,2]", [1, 2]],
    ['{"a":1}', { a: 1 }],
    ["'q'", "q"],
    ['"dq"', "dq"],
    ["[oops", "[oops"],
    ["hello", "hello"],
    ["", ""],
    ["0x10", "0x10"],
    ["1,2", "1,2"],
    ["'", "'"],
  ]) {
    test(`test_coerce[${JSON.stringify(raw)}]`, () => {
      assert.deepEqual(norm(ArgReader._coerce_scalar(raw)), expected);
    });
  }

  test("test_non_string_passthrough", () => {
    for (const v of [1, 2.5, null, [1], { a: 1 }, true]) assert.equal(ArgReader._coerce_scalar(v), v);
  });

  test("test_whitespace_preserved_for_plain_strings", () => {
    assert.equal(ArgReader._coerce_scalar("  spaced  "), "  spaced  ");
  });

  test("test_surprising_numeric_coercions", () => {
    // Documented footguns: Python's int()/float() accept these.
    assert.equal(ArgReader._coerce_scalar("1_000"), 1000);
    assert.equal(ArgReader._coerce_scalar("007"), 7);
    assert.ok(Number.isNaN(Number(ArgReader._coerce_scalar("nan"))));
    assert.equal(Number(ArgReader._coerce_scalar("inf")), Infinity);
    assert.equal(Number(ArgReader._coerce_scalar("infinity")), Infinity);
  });

  test("test_json_inner_values_not_coerced", () => {
    assert.deepEqual(ArgReader._coerce_scalar('["1", "true"]'), ["1", "true"]);
  });

  test("test_flatten_one_level", () => {
    const out = ArgReader._flatten_one_level({ a: "1", db: { host: "h", port: "5", deep: { k: "v" } } });
    assert.deepEqual(out, { a: 1, db_host: "h", db_port: 5, db_deep: { k: "v" } });
  });

  test("test_flatten_key_collision", () => {
    const out = ArgReader._flatten_one_level({ a_b: "1", a: { b: "2" } });
    assert.deepEqual(out, { a_b: 2 });
  });
});

describe("TestArgReaderSources", () => {
  /** ``target`` / ``reader`` fixtures. */
  const tr = (name, fn, opts = undefined) =>
    test(name, opts ?? {}, () =>
      macrotask(() => {
        const target = new DotMap();
        const reader = new ArgReader(target);
        return fn(reader, target, tmp_path());
      }),
    );

  tr("test_from_json", (reader, target, tmp) => {
    const p = path.join(tmp, "a.json");
    write_text(p, JSON.stringify({ x: 1, db: { host: "h", port: "5432" }, lst: ["1"], flag: "true" }));
    reader.from_json(p);
    assert.ok(target.x === 1 && target.db_host === "h" && target.db_port === 5432);
    assert.deepEqual([...target.lst], ["1"]);
    assert.equal(target.flag, true);
  });

  tr("test_from_json_non_object", (reader, _target, tmp) => {
    const p = path.join(tmp, "a.json");
    write_text(p, "[1,2]");
    assert.throws(() => reader.from_json(p), E.ValueError);
  });

  tr("test_from_json_missing", (reader, _target, tmp) => {
    assert.throws(() => reader.from_json(path.join(tmp, "missing.json")), E.FileNotFoundError);
  });

  tr("test_from_json_invalid", (reader, _target, tmp) => {
    const p = path.join(tmp, "a.json");
    write_text(p, "{bad");
    assert.throws(() => reader.from_json(p), E.JSONDecodeError);
  });

  tr("test_from_toml", (reader, target, tmp) => {
    const p = path.join(tmp, "a.toml");
    write_text(p, 'x = 1\nname = "n"\n[db]\nhost = "h"\nport = 5432\n[db.deep]\nk = "v"\n');
    reader.from_toml(p);
    assert.ok(target.x === 1 && target.name === "n");
    assert.ok(target.db_host === "h" && target.db_port === 5432);
    assert.deepEqual({ ...target.db_deep }, { k: "v" }); // smol-toml tables are null-prototype objects
  });

  tr("test_from_toml_invalid", (reader, _target, tmp) => {
    const p = path.join(tmp, "a.toml");
    write_text(p, "x = = 1");
    // ``tomllib.TOMLDecodeError`` -> smol-toml's ``TomlError``.
    assert.throws(
      () => reader.from_toml(p),
      (e) => e?.constructor?.name === "TomlError",
    );
  });

  tr("test_from_env", (reader, target, tmp) => {
    const p = path.join(tmp, "a.env");
    write_text(p, '# comment\nA=1\nB = "two"\nC=\nbad line\nD=x=y\n\n');
    reader.from_env(p);
    assert.ok(target.A === 1 && target.B === "two" && target.C === "" && target.D === "x=y");
    assert.ok(!("bad line" in target));
  });

  tr("test_from_env_export_prefix_not_stripped", (reader, target, tmp) => {
    const p = path.join(tmp, "a.env");
    write_text(p, "export E=5\n");
    reader.from_env(p);
    assert.equal(target.__getitem__("export E"), 5);
  });

  tr("test_from_xml", (reader, target, tmp) => {
    const p = path.join(tmp, "a.xml");
    write_text(p, "<root><x>1</x><db><host>h</host><port>5432</port></db><empty/></root>");
    reader.from_xml(p);
    assert.ok(target.x === 1 && target.db_host === "h" && target.db_port === 5432);
    assert.equal(target.__getitem__("empty"), "");
  });

  tr("test_dotmap_method_names_shadow_keys", (reader, target) => {
    // DotMap exposes ``empty()``/``get``/``items`` as methods, so args
    // named like them are only reachable via item access.
    reader.from_terminal(["empty=1", "items=2"]);
    assert.ok(target.__getitem__("empty") === 1 && target.__getitem__("items") === 2);
    assert.ok(typeof target.empty === "function" && typeof target.items === "function");
  });

  tr(
    "test_from_xml_invalid",
    (reader, _target, tmp) => {
      const p = path.join(tmp, "a.xml");
      write_text(p, "<root><x>1</root>");
      assert.throws(() => reader.from_xml(p), E.ParseError);
    },
  );

  tr("test_from_xml_deep_nesting_dropped", (reader, target, tmp) => {
    const p = path.join(tmp, "a.xml");
    write_text(p, "<root><db><deep><k>v</k></deep></db></root>");
    reader.from_xml(p);
    // Grandchildren with children collapse to their text ('' here).
    assert.equal(target.db_deep, "");
  });

  tr("test_from_terminal_tokens", (reader, target) => {
    reader.from_terminal(["a=1", "--b=2", "c", "=d", "e=f=g", "  h =  7 "]);
    assert.ok(target.a === 1 && target.__getitem__("--b") === 2 && target.e === "f=g" && target.h === 7);
    assert.ok(!("c" in target) && !("" in target));
  });

  tr("test_from_terminal_default_argv", (reader, target) => {
    const saved = process.argv;
    process.argv = [process.argv[0], "prog", "zz=3"]; // monkeypatch sys.argv = ["prog", "zz=3"]
    try {
      reader.from_terminal();
    } finally {
      process.argv = saved;
    }
    assert.equal(target.zz, 3);
  });

  tr("test_from_terminal_empty", (reader, target) => {
    reader.from_terminal([]);
    assert.equal(target.__len__(), 0);
  });

  tr("test_load_dispatch", (reader, target, tmp) => {
    for (const [suffix, body] of [
      [".json", '{"j": 1}'],
      [".toml", "t = 2\n"],
      [".env", "e=3\n"],
      [".xml", "<r><x>4</x></r>"],
    ]) {
      write_text(path.join(tmp, `f${suffix}`), body);
      reader.load(path.join(tmp, `f${suffix}`));
    }
    assert.deepEqual([target.j, target.t, target.e, target.x], [1, 2, 3, 4]);
  });

  tr("test_load_suffix_case_insensitive", (reader, target, tmp) => {
    const p = path.join(tmp, "F.JSON");
    write_text(p, '{"j": 1}');
    reader.load(p);
    assert.equal(target.j, 1);
  });

  tr("test_load_terminal", (reader, target) => {
    reader.load("terminal", { terminal_args: ["z=9"] });
    assert.equal(target.z, 9);
    reader.load("TERMINAL", { terminal_args: ["y=8"] });
    assert.equal(target.y, 8);
  });

  tr("test_load_unsupported", (reader, _target, tmp) => {
    assert.throws(() => reader.load(path.join(tmp, "a.yaml")), E.ValueError);
    assert.throws(() => reader.load("not-a-source"), E.ValueError);
  });

  tr("test_later_sources_override", (reader, target, tmp) => {
    const p = path.join(tmp, "a.json");
    write_text(p, '{"x": 1}');
    reader.load(p);
    reader.load("terminal", { terminal_args: ["x=2"] });
    assert.equal(target.x, 2);
  });

  tr("test_clear", (reader, target) => {
    reader.from_terminal(["a=1", "b=2"]);
    reader.clear();
    assert.equal(target.__len__(), 0);
  });

  t("test_clear_on_plain_object", () => {
    class Obj {}

    const o = new Obj();
    const r = new ArgReader(o);
    r.from_terminal(["a=1"]);
    assert.equal(o.a, 1);
    r.clear(); // no keys() -> no-op
    assert.equal(o.a, 1);
  });

  tr("test_methods_return_none", (reader) => {
    assert.ok(reader.from_terminal(["a=1"]) == null);
    assert.ok(reader.load("terminal", { terminal_args: ["b=1"] }) == null);
    assert.ok(reader.clear() == null);
  });
});

describe("TestModuleProperties", () => {
  test("test_runtime_property_returns_submodule", () => {
    assert.equal(laila.runtime, runtime_module);
  });

  test("test_import_runtime_is_warning_free", { skip: "CPython import machinery (sys.modules / importlib.import_module / ImportWarning) has no ESM counterpart" }, () => {});

  tl("test_logger_property_is_singleton", (logger) => {
    assert.equal(laila.logger, logger);
  });

  tp("test_command_memory_properties_track_active_policy", (fresh_policy) => {
    assert.equal(laila.command, fresh_policy.central.command);
    assert.equal(laila.memory, fresh_policy.central.memory);
  });
});

describe("TestLailaArgs", () => {
  /** autouse ``_restore`` fixture. */
  const with_restore = (fn) => {
    // ``environment`` is the live runtime mirror: re-assigning it would
    // trigger a full process reload, so it is deliberately left alone.
    const saved = Object.fromEntries([...laila.args.items()].filter(([k]) => k !== "environment"));
    const restore = () => {
      for (const k of [...laila.args.keys()]) {
        if (k !== "environment" && k.startsWith("deep_")) laila.args.__delitem__(k);
      }
      for (const [k, v] of Object.entries(saved)) laila.args.__setitem__(k, v);
    };
    let out;
    try {
      out = fn();
    } catch (e) {
      restore();
      throw e;
    }
    if (out !== null && out !== undefined && typeof out.then === "function") return out.then(restore, (e) => (restore(), Promise.reject(e))).then(() => out);
    restore();
    return out;
  };
  const ta = (name, fn) => test(name, () => with_restore(() => macrotask(fn)));
  const tap = (name, fn) => test(name, () => with_restore(() => with_fresh_policy(fn)));

  tap("test_environment_key_is_live_mirror", (fresh_policy) => {
    const env = laila.args.environment;
    assert.ok(env instanceof DotMap);
    assert.ok(dict_has(env.policies, fresh_policy.global_id));
  });

  tap("test_clear_wipes_environment_mirror", () => {
    // Documented footgun: ``laila.arg_reader.clear()`` deletes the runtime
    // snapshot too. It is lazily recreated on the next policy refresh.
    new ArgReader().clear();
    assert.ok(!("environment" in laila.args));
    laila.memorize(Entry.constant(1)).wait(WAIT);
    assert.ok("environment" in laila.args);
  });

  ta("test_default_target_is_laila_args", () => {
    new ArgReader().from_terminal(["deep_probe=11"]);
    assert.equal(laila.args.deep_probe, 11);
  });

  ta("test_missing_attribute_is_empty_dotmap", () => {
    const v = laila.args.deep_not_there;
    assert.ok(v instanceof DotMap);
    assert.equal(v.__len__(), 0);
  });

  ta("test_item_and_attr_access_equivalent", () => {
    new ArgReader().from_terminal(["deep_k=v"]);
    assert.ok(laila.args.__getitem__("deep_k") === laila.args.deep_k && laila.args.deep_k === "v");
  });

  tap("test_clear_default_target", () => {
    new ArgReader().from_terminal(["deep_a=1"]);
    new ArgReader().clear();
    assert.ok(!("deep_a" in laila.args));
  });

  ta("test_atomic_dotmap_target", () => {
    const ad = new AtomicDotMap();
    const r = new ArgReader(ad);
    r.from_terminal(["q=1", "n_x=2"]);
    assert.ok(ad.q === 1 && ad.n_x === 2);
    r.clear();
    assert.ok(![...ad.keys()].includes("q"));
  });

  ta("test_nested_json_into_atomic_dotmap", () => {
    const ad = new AtomicDotMap();
    const p = path.join(tmp_path(), "a.json");
    write_text(p, '{"db": {"host": "h", "deep": {"k": "v"}}}');
    new ArgReader(ad).from_json(p);
    assert.equal(ad.db_host, "h");
    assert.deepEqual(ad.db_deep, { k: "v" });
  });
});

// Tear down the lazily-activated default policy so the process exits.
test("zz teardown", () =>
  macrotask(() => {
    laila.terminate();
    fs.rmSync(TMP_ROOT, { recursive: true, force: true });
  }));
