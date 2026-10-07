/**
 * Port of ``tests/deep_eval/test_05_constitution_build_manifest_deep.py``.
 *
 * Deep tests for constitutions, the build pipeline, and Manifest blueprints.
 *
 * Constitution *source* is per-runtime: Python constitutions are Python
 * (``exec``), laila-js constitutions are JavaScript (``new Function``), so the
 * executed snippets below are the JS spelling of the Python bodies. Snippets
 * that are only stored / type-checked (never run) keep the Python text.
 */
import { test, describe } from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { S, laila, macrotask, run, with_fresh_policy } from "./_fixtures.js";

const T = await import(S + "_compat/pytypes.js");
const E = await import(S + "_compat/errors.js");
const pyjson = await import(S + "_compat/pyjson.js");
const { repr } = await import(S + "_compat/pyrepr.js");
const { BUILDER_MAP, build_by_scope, register_builder } = await import(S + "entry/constitution/build_maps.js");
const { ComplexConstitution } = await import(S + "entry/constitution/complex_constitution.js");
const { _REGISTRY, Constitution, _exec_one_fn } = await import(S + "entry/constitution/constitution.js");
const { SimpleConstitution } = await import(S + "entry/constitution/simple_constitution.js");
const { TransformationSequence } = await import(S + "entry/compdata/transformation/base.js");
const { Base64 } = await import(S + "entry/compdata/transformation/base64/base64.js");
const { Entry } = await import(S + "entry/entry.js");
const { EntryState } = await import(S + "entry/entry_state.js");
const { Manifest } = await import(S + "policy/central/memory/schema/manifest.js");
const { _RESOLVE_CHAIN, CyclicDependencyError } = await import(S + "policy/central/command/schema/parking.js");

const U1 = "11111111-2222-3333-4444-555555555555";
const U2 = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
const GID1 = `LAILA:ENTRY:${U1}`;
const GID2 = `LAILA:ENTRY:${U2}@evolution=0`;

const { NotImplemented, tuple } = T;
const str = (x) => T.str(x);
const t = (name, fn, opts) => (opts ? test(name, opts, () => macrotask(fn)) : test(name, () => macrotask(fn)));
const fp = (name, fn, opts) => t(name, () => with_fresh_policy(fn), opts);

const TMP_ROOT = fs.mkdtempSync(path.join(os.tmpdir(), "laila_deep_eval_05_"));
laila.set_default_directory(TMP_ROOT);

// Constitution bodies run in an empty scope (``exec(code, {})``); the Python
// bodies ``raise KeyError('k')`` / ``raise ValueError('boom')`` reach the
// builtins, so expose laila's exception classes the same way.
globalThis.__laila_deep_eval_E = E;

/**
 * Python ``m += other``: ``__iadd__`` first, then ``__add__``; ``NotImplemented``
 * from both is the interpreter's ``TypeError``.
 */
function iadd(m, other) {
  let r = m.__iadd__(other);
  if (r === NotImplemented) {
    r = typeof m.__add__ === "function" ? m.__add__(other) : NotImplemented;
    if (r === NotImplemented) throw new E.TypeError(`unsupported operand type(s) for +=: '${T.type_name(m)}' and '${T.type_name(other)}'`);
  }
  return r;
}

// ---------------------------------------------------------------------------
// _exec_one_fn
// ---------------------------------------------------------------------------

describe("TestExecOneFn", () => {
  test("test_single_function", () => {
    assert.equal(_exec_one_fn("function f(x) { return x + 1; }")(1), 2);
  });

  test("test_lambda_assignment", () => {
    assert.equal(_exec_one_fn("const g = (x) => x * 2;")(2), 4);
  });

  test("test_zero_functions_rejected", () => {
    assert.throws(() => _exec_one_fn("const x = 1;"), E.ValueError);
  });

  test("test_two_functions_rejected", () => {
    assert.throws(() => _exec_one_fn("function a(x) { return x; }\nfunction b(x) { return x; }"), E.ValueError);
  });

  test("test_module_import_not_counted", () => {
    // ``import math`` binds a module (not a callable) -> ``const math = Math``
    const fn = _exec_one_fn("const math = Math;\nfunction f(x) { return math.sqrt(x); }");
    assert.equal(fn(4), 2);
  });

  test("test_from_import_counts_as_callable", () => {
    // Documents: ``from math import sqrt`` makes two callables -> rejected.
    assert.throws(() => _exec_one_fn("const sqrt = Math.sqrt;\nfunction f(x) { return sqrt(x); }"), E.ValueError);
  });

  test("test_class_definition_counts_as_callable", () => {
    assert.throws(() => _exec_one_fn("class A {}\nfunction f(x) { return x; }"), E.ValueError);
  });

  test(
    "test_syntax_error_propagates",
    () => {
      assert.throws(() => _exec_one_fn("function f(x) return x"), E.SyntaxError);
    },
  );

  test("test_dunder_names_ignored", () => {
    const fn = _exec_one_fn("const __helper__ = () => 0;\nfunction f(x) { return x; }");
    assert.equal(fn(3), 3);
  });
});

// ---------------------------------------------------------------------------
// SimpleConstitution
// ---------------------------------------------------------------------------

describe("TestSimpleConstitution", () => {
  test("test_registered", () => {
    assert.equal(_REGISTRY["simple"], SimpleConstitution);
    assert.equal(SimpleConstitution._kind, "simple");
  });

  test("test_empty_is_identity", () => {
    assert.equal(new SimpleConstitution().build(5), 5);
    assert.equal(new SimpleConstitution({ codes: [] }).build(null), null);
  });

  test("test_sequential", () => {
    const c = new SimpleConstitution({ codes: ["function a(x) { return x + 1; }", "function b(x) { return x * 10; }"] });
    assert.equal(c.build(1), 20);
  });

  test("test_codes_copy", () => {
    const src = ["function a(x) { return x; }"];
    const c = new SimpleConstitution({ codes: src });
    src.push("function b(x) { return x; }");
    assert.equal(c.codes.length, 1);
    c.codes.push("x");
    assert.equal(c.codes.length, 1);
  });

  for (const [id, bad] of [
    ["'def f(x): return x'", "def f(x): return x"],
    ["[1]", [1]],
    ["[None]", [null]],
  ]) {
    test(`test_codes_type_checked[${id}]`, () => {
      assert.throws(() => new SimpleConstitution({ codes: bad }), E.TypeError);
    });
  }
  test(
    "test_codes_type_checked[('def f(x): return x',)]",
    () => {
      assert.throws(() => new SimpleConstitution({ codes: tuple(["def f(x): return x"]) }), E.TypeError);
    },
  );

  test("test_as_dict", () => {
    const d = new SimpleConstitution({ codes: ["def f(x): return x"] }).as_dict();
    assert.equal(d["_kind"], "simple");
    assert.deepEqual(d["codes"], ["def f(x): return x"]);
    pyjson.dumps(d);
  });

  test("test_from_dict_roundtrip", () => {
    const c = new SimpleConstitution({ codes: ["function f(x) { return x + 1; }"] });
    const c2 = Constitution.from_dict(c.as_dict());
    assert.ok(c2 instanceof SimpleConstitution);
    assert.deepEqual(c2.codes, c.codes);
    assert.equal(c2.build(1), 2);
  });

  test("test_from_dict_missing_codes", () => {
    const c = Constitution.from_dict({ _kind: "simple" });
    assert.deepEqual(c.codes, []);
  });

  test("test_from_dict_none", () => {
    assert.equal(Constitution.from_dict(null), null);
  });

  test("test_from_dict_missing_kind", () => {
    assert.throws(() => Constitution.from_dict({ codes: [] }), E.KeyError);
  });

  test("test_from_dict_unknown_kind", () => {
    assert.throws(() => Constitution.from_dict({ _kind: "nope" }), E.ValueError);
  });

  test("test_build_error_propagates", () => {
    const c = new SimpleConstitution({ codes: ["function f(x) { throw new globalThis.__laila_deep_eval_E.KeyError('k'); }"] });
    assert.throws(() => c.build(1), E.KeyError);
  });

  test(
    "test_abstract_base",
    () => {
      assert.throws(() => new Constitution(), E.TypeError);
    },
  );
});

// ---------------------------------------------------------------------------
// ComplexConstitution
// ---------------------------------------------------------------------------

describe("TestComplexConstitution", () => {
  test("test_registered", () => {
    assert.equal(_REGISTRY["complex"], ComplexConstitution);
  });

  test("test_code_and_manifest", () => {
    const m = new Manifest({ data: { a: GID1 } });
    const c = new ComplexConstitution({ code: "def f(m): return 1", manifest: m });
    assert.equal(c.code, "def f(m): return 1");
    assert.equal(c.manifest, m);
    assert.equal(c.manifest_global_id, m.global_id);
  });

  test("test_code_immutable", () => {
    const c = new ComplexConstitution({ code: "def f(m): return 1" });
    assert.throws(() => {
      c.code = "def g(m): return 2";
    }, E.AttributeError);
  });

  test("test_code_type", () => {
    assert.throws(() => new ComplexConstitution({ code: 123 }), E.TypeError);
  });

  test("test_manifest_type", () => {
    assert.throws(() => new ComplexConstitution({ code: "def f(m): return 1", manifest: { a: GID1 } }), E.TypeError);
  });

  test("test_manifest_immutable", () => {
    const c = new ComplexConstitution({ code: "def f(m): return 1", manifest: new Manifest({ data: { a: GID1 } }) });
    assert.throws(() => {
      c.manifest = new Manifest({ data: { b: GID1 } });
    }, E.AttributeError);
  });

  test("test_manifest_global_id_only", () => {
    const c = new ComplexConstitution({ code: "def f(m): return 1", manifest_global_id: "LAILA:MANIFEST:" + U1 });
    assert.equal(c.manifest, null);
    assert.equal(c.manifest_global_id, "LAILA:MANIFEST:" + U1);
  });

  test("test_as_dict", () => {
    const m = new Manifest({ data: { a: GID1 } });
    const d = new ComplexConstitution({ code: "def f(m): return 1", manifest: m }).as_dict();
    assert.deepEqual(d, { _kind: "complex", code: "def f(m): return 1", manifest_global_id: m.global_id });
  });

  test("test_from_dict", () => {
    const d = { _kind: "complex", code: "def f(m): return 1", manifest_global_id: "LAILA:MANIFEST:" + U1 };
    const c = Constitution.from_dict(d);
    assert.ok(c instanceof ComplexConstitution);
    assert.equal(c.code, d["code"]);
    assert.equal(c.manifest, null);
    assert.equal(c.manifest_global_id, d["manifest_global_id"]);
  });

  test("test_build_without_code", () => {
    assert.throws(() => new ComplexConstitution({ manifest: new Manifest({ data: { a: GID1 } }) }).build(), E.RuntimeError);
  });

  test("test_build_without_manifest", () => {
    assert.throws(() => new ComplexConstitution({ code: "def f(m): return 1" }).build(), E.RuntimeError);
  });

  test("test_resolve_manifest_none", () => {
    assert.equal(new ComplexConstitution({ code: "def f(m): return 1" })._resolve_manifest_sync(), null);
  });
});

// ---------------------------------------------------------------------------
// build_by_scope
// ---------------------------------------------------------------------------

describe("TestBuildByScope", () => {
  test("test_entry_and_manifest_registered", () => {
    assert.ok("ENTRY" in BUILDER_MAP);
    assert.ok("MANIFEST" in BUILDER_MAP);
  });

  test("test_entry_passthrough", () => {
    const e = Entry.constant(1);
    assert.equal(build_by_scope(e), e);
  });

  t("test_entry_passthrough_async", () => {
    const e = Entry.constant(1);
    assert.equal(run(build_by_scope(e, { asynchronous: true })), e);
  });

  test("test_json_string_input", () => {
    const d = Entry.constant([1]).serialize(new TransformationSequence({ transformations: [new Base64()] }));
    const out = build_by_scope(pyjson.dumps(d));
    assert.deepEqual(out.data, [1]);
  });

  test("test_dict_input", () => {
    const d = Entry.constant([1]).serialize(new TransformationSequence());
    assert.deepEqual(build_by_scope(d).data, [1]);
  });

  test("test_invalid_input", () => {
    assert.throws(() => build_by_scope(123), E.RuntimeError);
  });

  test("test_unknown_scope", () => {
    assert.throws(() => build_by_scope({ _scopes: ["NOPE"], _uuid: U1 }), E.ValueError);
  });

  test("test_missing_scopes_defaults_entry", () => {
    const d = Entry.constant([1]).serialize(new TransformationSequence());
    delete d["_scopes"];
    assert.deepEqual(build_by_scope(d).data, [1]);
  });

  test("test_register_builder_custom", () => {
    register_builder(
      "DEEPTEST",
      (_d, _k) => "sync",
      (_d, _k) => "async",
    );
    try {
      assert.equal(build_by_scope({ _scopes: ["DEEPTEST"] }), "sync");
      assert.equal(build_by_scope({ _scopes: ["DEEPTEST"] }, { asynchronous: true }), "async");
    } finally {
      delete BUILDER_MAP["DEEPTEST"];
    }
  });

  test("test_only_first_scope_consulted", () => {
    // Documents: nested scopes dispatch on the first segment only.
    const d = Entry.contingent({ uuid: U1, scopes: ["ENTRY", "SUB"], data: [1], state: EntryState.READY }).serialize(new TransformationSequence());
    const out = build_by_scope(d);
    assert.deepEqual(out.scopes, ["ENTRY", "SUB"]);
  });

  test("test_custom_first_scope_cannot_be_rebuilt", () => {
    // Documents: an entry with a custom leading scope is not rebuildable from storage.
    const d = Entry.contingent({ uuid: U1, scopes: ["CUSTOM"], data: [1], state: EntryState.READY }).serialize(new TransformationSequence());
    assert.throws(() => build_by_scope(d), E.ValueError);
  });
});

// ---------------------------------------------------------------------------
// Entry build paths
// ---------------------------------------------------------------------------

describe("TestEntryBuild", () => {
  test("test_build_sync_simple", () => {
    const e = Entry.contingent({ constitution: ["function f(x) { return x * 2; }"], data: 4 });
    assert.equal(e.state, EntryState.STAGED);
    const out = e._build_sync();
    assert.equal(out, e);
    assert.equal(e.data, 8);
    assert.equal(e.state, EntryState.READY);
    assert.equal(e.constitution, null);
    assert.equal(e.locally_modified, false);
  });

  t("test_build_async_simple", () => {
    const e = Entry.contingent({ constitution: ["function f(x) { return [x]; }"], data: 1 });
    run(e._build_async());
    assert.deepEqual(e.data, [1]);
  });

  test("test_build_already_ready_raises", () => {
    assert.throws(() => Entry.constant(1)._build_sync(), E.RuntimeError);
  });

  test("test_build_without_constitution_raises", () => {
    const e = Entry.contingent({ data: 1 });
    assert.throws(() => e._build_sync(), E.RuntimeError);
    assert.throws(() => e._build_inplace(), E.RuntimeError);
  });

  test("test_build_none_input", () => {
    const e = Entry.contingent({ constitution: ["function f(x) { return x === null; }"] });
    e._build_sync();
    assert.equal(e.data, true);
  });

  test("test_build_returning_none", () => {
    const e = Entry.contingent({ constitution: ["function f(x) { return null; }"], data: 1 });
    e._build_sync();
    assert.equal(e.data, null);
    assert.equal(e.state, EntryState.READY);
  });

  t("test_build_dispatch_flag", () => {
    const e = Entry.contingent({ constitution: ["function f(x) { return 1; }"] });
    assert.equal(e._build({ asynchronous: false }), e);
    const e2 = Entry.contingent({ constitution: ["function f(x) { return 1; }"] });
    assert.equal(run(e2._build({ asynchronous: true })), e2);
  });

  fp("test_laila_build_simple", () => {
    const e = Entry.contingent({ constitution: ["function f(x) { return x + 1; }"], data: 1 });
    const fut = laila.build(e);
    fut.wait(10);
    assert.equal(e.data, 2);
    assert.ok(fut.data === e || fut.result === e);
  });

  fp("test_laila_build_complex", () => {
    const x = Entry.constant(2);
    const y = Entry.variable(3);
    laila.memorize([x, y]).wait(10);
    const m = new Manifest({ data: { x, y } });
    const e = Entry.variable(null, { constitution: "function f(m) {\n  const d = m.realized;\n  return d.x.data * d.y.data;\n}\n", manifest: m });
    laila.build(e).wait(10);
    assert.equal(e.data, 6);
    assert.equal(e.state, EntryState.READY);
    assert.equal(e.constitution, null);
  });

  fp("test_laila_build_complex_unmemorized_deps_fails", () => {
    // Documents: building requires the manifest's entries to be memorized first.
    const x = Entry.constant(2);
    const m = new Manifest({ data: { x } });
    const e = Entry.variable(null, { constitution: "function f(m) {\n  return m.realized.x.data;\n}\n", manifest: m });
    const fut = laila.build(e);
    assert.throws(() => fut.wait(10), E.KeyError);
  });

  fp("test_documented_build_body_example", () => {
    const x = Entry.constant(2);
    const y = Entry.constant(3);
    laila.memorize([x, y]).wait(10);
    // Exactly the example in the Entry.variable docstring.
    const src = "function f(m) { return m.realized.x.data + m.realized.y.data; }";
    const e = Entry.variable(null, { constitution: src, manifest: new Manifest({ data: { x, y } }) });
    laila.build(e).wait(10);
    assert.equal(e.data, 5);
  });

  fp("test_laila_build_complex_body_error_propagates", () => {
    const x = Entry.constant(2);
    laila.memorize(x).wait(10);
    const e = Entry.variable(null, {
      constitution: "function f(m) {\n  throw new globalThis.__laila_deep_eval_E.ValueError('boom');\n}\n",
      manifest: new Manifest({ data: { x } }),
    });
    const fut = laila.build(e);
    assert.throws(() => fut.wait(10), E.ValueError);
    assert.equal(e.state, EntryState.STAGED);
  });

  fp("test_build_from_remembered_manifest_gid", () => {
    const x = Entry.constant(10);
    const m = new Manifest({ data: { x } });
    laila.memorize(m).wait(10);
    const c = new ComplexConstitution({ code: "function f(m) {\n  return m.realized.x.data + 1;\n}\n", manifest_global_id: m.global_id });
    const e = Entry.contingent({ constitution: c, evolution: 0 });
    laila.build(e).wait(10);
    assert.equal(e.data, 11);
  });

  fp("test_cycle_detection", () => {
    const e = Entry.contingent({ constitution: ["function f(x) { return 1; }"] });
    const token = _RESOLVE_CHAIN.set([e.global_id]);
    try {
      assert.throws(() => laila.build(e), CyclicDependencyError);
    } finally {
      _RESOLVE_CHAIN.reset(token);
    }
  });
});

// ---------------------------------------------------------------------------
// Manifest blueprint semantics
// ---------------------------------------------------------------------------

describe("TestManifestBlueprint", () => {
  test("test_empty", () => {
    const m = new Manifest();
    assert.equal(m.data, null);
    assert.equal(m.blueprint, null);
    assert.equal(T.len(m), 0);
    assert.deepEqual([...m], []);
    assert.ok(!m.__contains__(GID1));
  });

  test("test_from_strings", () => {
    const m = new Manifest({ data: { a: GID1, b: [GID2] } });
    assert.deepEqual(m.data, { a: GID1, b: [GID2] });
    assert.equal(m._pending_entries, null);
    assert.deepEqual([...m], [GID1, GID2]);
    assert.ok(m.__contains__(GID1) && m.__contains__(GID2));
  });

  test("test_blueprint_alias", () => {
    assert.deepEqual(new Manifest({ blueprint: { a: GID1 } }).data, { a: GID1 });
  });

  test("test_from_entries", () => {
    const x = Entry.constant(1);
    const y = Entry.variable(2);
    const m = new Manifest({ data: { x, nested: { y: [y] } } });
    assert.deepEqual(m.data, { x: x.global_id, nested: { y: [y.global_id] } });
    assert.equal(m._pending_entries.length, 2);
    assert.equal(m._pending_entries[0], x);
    assert.equal(m._pending_entries[1], y);
  });

  test("test_mixed_rejected", () => {
    assert.throws(() => new Manifest({ data: { x: Entry.constant(1), y: GID1 } }), E.ValueError);
  });

  test("test_invalid_value_type", () => {
    assert.throws(() => new Manifest({ data: { x: 123 } }), (e) => e instanceof E.ValueError || e instanceof E.TypeError);
  });

  test("test_identity", () => {
    const m = new Manifest({ data: { a: GID1 }, nickname: "mani" });
    assert.deepEqual(m.scopes, ["MANIFEST"]);
    assert.equal(m.evolution, null);
    assert.equal(m.uuid, Entry.generate_uuid_from_nickname("mani"));
  });

  test("test_explicit_uuid", () => {
    assert.equal(new Manifest({ uuid: U1 }).uuid, U1);
  });

  test("test_evolution_forced_none", () => {
    assert.equal(new Manifest({ data: { a: GID1 }, evolution: 3 }).evolution, null);
  });

  test("test_state_ready_with_blueprint", () => {
    assert.equal(new Manifest({ data: { a: GID1 } }).state, EntryState.READY);
  });

  test("test_state_documented_as_na_but_is_not", () => {
    // Documents a doc/impl mismatch: EntryState docs say Manifest is pinned to NA.
    const m = new Manifest({ data: { a: GID1 } });
    assert.equal(Manifest._ALLOWS_NON_NA_STATE, true);
    assert.notEqual(m.state, EntryState.NA);
  });

  test("test_data_is_deep_copied", () => {
    const src = { a: [GID1] };
    const m = new Manifest({ data: src });
    src["a"].push(GID2);
    assert.deepEqual(m.data, { a: [GID1] });
  });

  test("test_getitem", () => {
    const m = new Manifest({ data: { a: GID1 } });
    assert.equal(m["a"], GID1);
    assert.throws(() => m.__getitem__("zzz"), E.KeyError);
  });

  test("test_getitem_empty", () => {
    assert.throws(() => new Manifest().__getitem__("a"), E.KeyError);
  });

  test("test_keys_values_items", () => {
    const m = new Manifest({ data: { a: GID1 } });
    assert.deepEqual([...m.keys()], ["a"]);
    assert.deepEqual([...m.values()], [GID1]);
    assert.deepEqual(
      [...m.items()].map((kv) => [...kv]),
      [["a", GID1]],
    );
  });

  test("test_keys_empty", () => {
    assert.deepEqual([...new Manifest().keys()], []);
  });

  test("test_iter_depth_first_order", () => {
    const m = new Manifest({ data: { a: GID1, b: { c: [GID2, GID1] } } });
    assert.deepEqual([...m], [GID1, GID2, GID1]);
  });

  test("test_contains_non_str", () => {
    assert.ok(!new Manifest({ data: { a: GID1 } }).__contains__(5));
  });

  test("test_sub_manifest", () => {
    const m = new Manifest({ data: { a: GID1, b: GID2 } });
    const s = m.sub_manifest(["a"]);
    assert.deepEqual(s.data, { a: GID1 });
    assert.notEqual(s.global_id, m.global_id);
  });

  test("test_sub_manifest_missing_key", () => {
    assert.throws(() => new Manifest({ data: { a: GID1 } }).sub_manifest(["zzz"]), E.KeyError);
  });

  test("test_extend", () => {
    const m = new Manifest({ data: { a: GID1 } });
    m.extend(new Manifest({ data: { b: GID2 } }));
    assert.deepEqual(m.data, { a: GID1, b: GID2 });
  });

  test("test_extend_overlap_rejected", () => {
    const m = new Manifest({ data: { a: GID1 } });
    assert.throws(() => m.extend(new Manifest({ data: { a: GID2 } })), E.KeyError);
  });

  test("test_extend_overwrite", () => {
    const m = new Manifest({ data: { a: GID1 } });
    m.extend(new Manifest({ data: { a: GID2 } }), { overwrite: true });
    assert.deepEqual(m.data, { a: GID2 });
  });

  test("test_extend_type", () => {
    assert.throws(() => new Manifest().extend({ a: GID1 }), E.TypeError);
  });

  test("test_extend_into_empty", () => {
    const m = new Manifest();
    m.extend(new Manifest({ data: { a: GID1 } }));
    assert.deepEqual(m.data, { a: GID1 });
  });

  test("test_extend_merges_pending", () => {
    const [x, y] = [Entry.constant(1), Entry.constant(2)];
    const m = new Manifest({ data: { x } });
    m.extend(new Manifest({ data: { y } }));
    assert.equal(m._pending_entries.length, 2);
    assert.equal(m._pending_entries[0], x);
    assert.equal(m._pending_entries[1], y);
  });

  test("test_iadd", () => {
    let m = new Manifest({ data: { a: GID1 } });
    m = iadd(m, new Manifest({ data: { b: GID2 } }));
    assert.deepEqual(m.data, { a: GID1, b: GID2 });
  });

  test("test_iadd_overlap", () => {
    const m = new Manifest({ data: { a: GID1 } });
    assert.throws(() => iadd(m, new Manifest({ data: { a: GID2 } })), E.KeyError);
  });

  test("test_iadd_non_manifest", () => {
    const m = new Manifest({ data: { a: GID1 } });
    assert.throws(() => iadd(m, 1), E.TypeError);
  });

  test("test_floating_leaves", () => {
    const m = new Manifest({ data: { a: GID1, b: `LAILA:ENTRY:${U1}@evolution=-1`, c: `LAILA:ENTRY:${U1}@creation_timestamp=2026` } });
    assert.deepEqual(m.floating_leaves(), [`LAILA:ENTRY:${U1}@evolution=-1`, `LAILA:ENTRY:${U1}@creation_timestamp=2026`]);
  });

  test("test_repr", () => {
    const m = new Manifest({ data: { a: GID1, b: [GID2] } });
    assert.equal(repr(m), `Manifest(${m.global_id}, entries=2)`);
    assert.equal(str(m), m.global_id);
  });

  test("test_as_dict_and_from_dict", () => {
    const m = new Manifest({ data: { a: GID1 }, uuid: U1 });
    const d = m.as_dict();
    assert.deepEqual(d["_scopes"], ["MANIFEST"]);
    assert.deepEqual(d["payload"], { a: GID1 });
    const m2 = Manifest.from_dict(d);
    assert.ok(m2 instanceof Manifest);
    assert.deepEqual(m2.data, { a: GID1 });
    assert.equal(m2.global_id, m.global_id);
  });

  test("test_serialize_rebuild", () => {
    const m = new Manifest({ data: { a: GID1, n: { b: [GID2] } }, uuid: U1 });
    const d = m.serialize(new TransformationSequence());
    const out = build_by_scope(d);
    assert.ok(out instanceof Manifest);
    assert.deepEqual(out.data, m.data);
  });

  fp("test_realized_empty_manifest_raises", () => {
    assert.throws(() => new Manifest().realized, E.RuntimeError);
  });

  fp("test_realized_structure", () => {
    const [x, y] = [Entry.constant(1), Entry.variable(2)];
    laila.memorize([x, y]).wait(10);
    const m = new Manifest({ data: { x, n: { ys: [y] } } });
    const r = m.realized;
    assert.equal(r["x"].data, 1);
    assert.equal(r["n"]["ys"][0].data, 2);
    assert.equal(r["x"].global_id, x.global_id);
  });

  fp("test_realized_missing_raises", () => {
    const m = new Manifest({ data: { a: GID1 } });
    assert.throws(() => m.realized, E.KeyError);
  });

  fp("test_async_realized", () => {
    const x = Entry.constant(5);
    laila.memorize(x).wait(10);
    const m = new Manifest({ data: { x } });
    const r = run(m.async_realized);
    assert.equal(r["x"].data, 5);
  });

  fp("test_manifest_memorize_stores_pending_and_self", () => {
    const [x, y] = [Entry.constant(1), Entry.constant(2)];
    const m = new Manifest({ data: { x, y } });
    const g = laila.memorize(m);
    g.wait(10);
    assert.equal(laila.remember(x.global_id).data, 1);
    assert.deepEqual(laila.remember(m.global_id).data, m.data);
  });

  fp("test_manifest_remember_returns_entries_in_order", () => {
    const [x, y] = [Entry.constant("x"), Entry.constant("y")];
    const m = new Manifest({ data: { x, y } });
    laila.memorize(m).wait(10);
    const g = laila.remember(m);
    const out = g.result;
    assert.deepEqual(
      out.map((e) => e.data),
      ["x", "y"],
    );
    assert.deepEqual(g.data, ["x", "y"]);
  });

  fp("test_manifest_forget", () => {
    const x = Entry.constant(1);
    const m = new Manifest({ data: { x } });
    laila.memorize(m).wait(10);
    laila.forget(m).wait(10);
    assert.throws(() => laila.remember(x.global_id).wait(10), E.KeyError);
  });

  fp("test_manifest_in_list_memorized_as_plain_entry", () => {
    const x = Entry.constant(1);
    const m = new Manifest({ data: { x } });
    laila.memorize([m]).wait(10);
    // the manifest itself is stored...
    assert.deepEqual(laila.remember(m.global_id).data, m.data);
    // ...but its pending entries are not
    assert.throws(() => laila.remember(x.global_id).wait(10), E.KeyError);
  });
});

// Tear down the lazily-activated default policy so the process exits.
test("zz teardown", () =>
  macrotask(() => {
    delete globalThis.__laila_deep_eval_E;
    laila.terminate();
    fs.rmSync(TMP_ROOT, { recursive: true, force: true });
  }));
