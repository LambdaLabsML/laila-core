/**
 * Port of ``tests/deep_eval/test_03_entry_deep.py``.
 *
 * Deep tests for ``Entry`` -- factories, lifecycle, evolution, serialization.
 */
import { test, describe } from "node:test";
import assert from "node:assert/strict";

import { S, macrotask, run } from "./_fixtures.js";

const T = await import(S + "_compat/pytypes.js");
const E = await import(S + "_compat/errors.js");
const TH = await import(S + "_compat/threading.js");
const time = await import(S + "_compat/time.js");
const _uuid = await import(S + "_compat/uuid.js");
const pyjson = await import(S + "_compat/pyjson.js");
const { repr } = await import(S + "_compat/pyrepr.js");
const { NDArray } = await import(S + "_compat/ndarray.js");
const { _LAILA_IDENTIFIABLE_OBJECT } = await import(S + "basics/definitions/identifiable_object.js");
const { ComputationalData } = await import(S + "entry/compdata/taxonomy/compdata.js");
const { TransformationSequence } = await import(S + "entry/compdata/transformation/base.js");
const { Base64 } = await import(S + "entry/compdata/transformation/base64/base64.js");
const { Zlib } = await import(S + "entry/compdata/transformation/compression/zlib.js");
const { SimpleConstitution } = await import(S + "entry/constitution/simple_constitution.js");
const { Entry } = await import(S + "entry/entry.js");
const { EntryState } = await import(S + "entry/entry_state.js");
const { EntryNotBuiltError } = await import(S + "entry/exceptions.js");
const { Manifest } = await import(S + "policy/central/memory/schema/manifest.js");

const U1 = "11111111-2222-3333-4444-555555555555";

const { tuple } = T;
const str = (x) => T.str(x);
const t = (name, fn) => test(name, () => macrotask(fn));
const is_bytes = (x) => x instanceof Uint8Array;
const assert_array_equal = (got, expected) => {
  assert.ok(got instanceof NDArray, `${repr(got)} is not an NDArray`);
  assert.deepEqual([...got.shape], [...expected.shape]);
  assert.ok(got.array_equal(expected, { equal_nan: true }), `${repr(got)} != ${repr(expected)}`);
};

// ---------------------------------------------------------------------------
// Entry.constant
// ---------------------------------------------------------------------------

describe("TestConstant", () => {
  test("test_basic", () => {
    const e = Entry.constant({ a: 1 });
    assert.equal(e.evolution, null);
    assert.equal(e.state, EntryState.READY);
    assert.deepEqual(e.data, { a: 1 });
    assert.ok(!e.global_id.includes("@"));
    assert.deepEqual(e.scopes, ["ENTRY"]);
  });

  test("test_random_uuid4", () => {
    assert.equal(new _uuid.UUID(Entry.constant(1).uuid).version, 4);
  });

  test("test_two_constants_differ", () => {
    assert.notEqual(Entry.constant(1).global_id, Entry.constant(1).global_id);
  });

  test("test_explicit_uuid", () => {
    assert.equal(Entry.constant(1, { uuid: U1 }).uuid, U1);
  });

  test("test_nickname_deterministic", () => {
    const a = Entry.constant(1, { nickname: "k" });
    const b = Entry.constant(2, { nickname: "k" });
    assert.equal(a.global_id, b.global_id);
    assert.equal(a.uuid, _LAILA_IDENTIFIABLE_OBJECT.generate_uuid_from_nickname("k"));
  });

  test("test_global_id", () => {
    const e = Entry.constant(1, { global_id: `LAILA:ENTRY:${U1}` });
    assert.equal(e.uuid, U1);
    assert.equal(e.global_id, `LAILA:ENTRY:${U1}`);
  });

  test("test_global_id_and_uuid_conflict", () => {
    assert.throws(() => Entry.constant(1, { global_id: `LAILA:ENTRY:${U1}`, uuid: U1 }), E.RuntimeError);
  });

  test("test_global_id_with_evolution_rejected", () => {
    assert.throws(() => Entry.constant(1, { global_id: `LAILA:ENTRY:${U1}@evolution=0` }), E.RuntimeError);
  });

  test("test_invalid_global_id", () => {
    assert.throws(() => Entry.constant(1, { global_id: "LAILA:" }), E.ValueError);
  });

  test("test_global_id_custom_scopes_preserved", () => {
    const e = Entry.constant(1, { global_id: `LAILA:CUSTOM:${U1}` });
    assert.deepEqual(e.scopes, ["CUSTOM"]);
    assert.equal(e.global_id, `LAILA:CUSTOM:${U1}`);
  });

  test("test_nickname_wins_over_uuid_like_entry_init", () => {
    // Same precedence rule as Entry.__init__ (pinned by the functional suite).
    const e = Entry.constant(1, { uuid: U1, nickname: "n" });
    assert.equal(e.uuid, Entry.generate_uuid_from_nickname("n"));
    assert.equal(e.uuid, new Entry({ data: 1, uuid: U1, nickname: "n" }).uuid);
  });

  test("test_none_payload", () => {
    const e = Entry.constant(null);
    assert.equal(e.data, null);
    assert.equal(e.state, EntryState.READY);
  });

  test("test_payload_wrapped", () => {
    const e = Entry.constant([1, 2]);
    assert.ok(e._payload instanceof ComputationalData);
    assert.deepEqual(e.data, [1, 2]);
  });

  test("test_not_locally_modified_at_creation", () => {
    assert.equal(Entry.constant(1).locally_modified, false);
  });

  test("test_has_evolution_false", () => {
    assert.equal(Entry.constant(1).has_evolution(), false);
  });

  test("test_bump_noop_for_constant", () => {
    const e = Entry.constant(1);
    e.data = 2;
    assert.ok(e.locally_modified);
    assert.equal(e.bump_evolution_if_locally_modified(), false);
    assert.equal(e.evolution, null);
  });

  test("test_evolve_rejected", () => {
    assert.throws(() => Entry.constant(1).evolve(2), E.RuntimeError);
  });

  test("test_creation_timestamp_present", () => {
    assert.ok(Entry.constant(1).creation_timestamp);
  });
});

// ---------------------------------------------------------------------------
// Entry.variable
// ---------------------------------------------------------------------------

describe("TestVariable", () => {
  test("test_basic", () => {
    const e = Entry.variable([1]);
    assert.equal(e.evolution, 0);
    assert.equal(e.state, EntryState.READY);
    assert.ok(e.global_id.endsWith("@evolution=0"));
  });

  test("test_explicit_evolution", () => {
    assert.equal(Entry.variable(1, { evolution: 5 }).evolution, 5);
  });

  test("test_explicit_uuid", () => {
    assert.equal(Entry.variable(1, { uuid: U1 }).uuid, U1);
  });

  test("test_nickname", () => {
    const e = Entry.variable(1, { nickname: "v" });
    assert.equal(e.uuid, _LAILA_IDENTIFIABLE_OBJECT.generate_uuid_from_nickname("v"));
  });

  test("test_global_id_sets_uuid_and_evolution", () => {
    const e = Entry.variable(1, { global_id: `LAILA:ENTRY:${U1}@evolution=3` });
    assert.equal(e.uuid, U1);
    assert.equal(e.evolution, 3);
  });

  test("test_global_id_without_evolution_defaults_zero", () => {
    const e = Entry.variable(1, { global_id: `LAILA:ENTRY:${U1}` });
    assert.equal(e.evolution, 0);
  });

  test("test_global_id_conflicts", () => {
    assert.throws(() => Entry.variable(1, { global_id: `LAILA:ENTRY:${U1}`, uuid: U1 }), E.RuntimeError);
    assert.throws(() => Entry.variable(1, { global_id: `LAILA:ENTRY:${U1}`, evolution: 1 }), E.RuntimeError);
  });

  test("test_constitution_and_data_conflict", () => {
    assert.throws(() => Entry.variable(1, { constitution: "def f(m): return 1", manifest: {} }), E.RuntimeError);
  });

  test("test_constitution_without_manifest", () => {
    assert.throws(() => Entry.variable(null, { constitution: "def f(m): return 1" }), E.ValueError);
  });

  test("test_manifest_without_constitution", () => {
    assert.throws(() => Entry.variable(null, { manifest: {} }), E.ValueError);
  });

  test("test_state_override", () => {
    assert.equal(Entry.variable(1, { state: EntryState.STALE }).state, EntryState.STALE);
  });

  test("test_negative_evolution_accepted_without_validation", () => {
    // Documents: factory does not validate the evolution range.
    const e = Entry.variable(1, { evolution: -1 });
    assert.equal(e.evolution, -1);
    // ... and the produced global id cannot be parsed back
    assert.throws(() => _LAILA_IDENTIFIABLE_OBJECT.process_global_id(e.global_id), E.ValueError);
  });

  test("test_global_id_custom_scopes_preserved", () => {
    const e = Entry.variable(1, { global_id: `LAILA:CUSTOM:${U1}@evolution=1` });
    assert.deepEqual(e.scopes, ["CUSTOM"]);
  });

  test("test_has_evolution", () => {
    assert.ok(Entry.variable(1).has_evolution());
  });
});

// ---------------------------------------------------------------------------
// evolve / locally modified / bump
// ---------------------------------------------------------------------------

describe("TestEvolution", () => {
  test("test_evolve_returns_new_object", () => {
    const v = Entry.variable([1]);
    const v2 = v.evolve([1, 2]);
    assert.notEqual(v2, v);
    assert.equal(v2.uuid, v.uuid);
    assert.deepEqual(v2.scopes, v.scopes);
    assert.equal(v2.evolution, 1);
    assert.deepEqual(v2.data, [1, 2]);
    assert.equal(v2.state, EntryState.READY);
  });

  test("test_evolve_leaves_original", () => {
    const v = Entry.variable([1]);
    v.evolve([2]);
    assert.equal(v.evolution, 0);
    assert.deepEqual(v.data, [1]);
  });

  test("test_evolve_chain", () => {
    let v = Entry.variable(0);
    for (let i = 1; i < 6; i++) v = v.evolve(i);
    assert.equal(v.evolution, 5);
    assert.equal(v.data, 5);
  });

  test("test_evolve_none_payload", () => {
    const v = Entry.variable(1).evolve();
    assert.equal(v.data, null);
  });

  t("test_evolve_new_timestamp", () => {
    const v = Entry.variable(1);
    time.sleep(0.002);
    const v2 = v.evolve(2);
    assert.ok(v2.creation_timestamp >= v.creation_timestamp);
  });

  test("test_evolve_not_locally_modified", () => {
    assert.equal(Entry.variable(1).evolve(2).locally_modified, false);
  });

  test("test_evolved_global_id_differs_only_in_evolution", () => {
    const v = Entry.variable(1);
    const v2 = v.evolve(2);
    assert.equal(v.global_id.split("@")[0], v2.global_id.split("@")[0]);
    assert.notEqual(v.global_id, v2.global_id);
  });

  test("test_evolve_unbuilt_constitution_not_implemented", () => {
    const dep = Entry.constant(1);
    const e = Entry.variable(null, { constitution: "def f(m):\n    return m['x'].data\n", manifest: new Manifest({ data: { x: dep } }) });
    assert.throws(() => e.evolve(1), E.NotImplementedError);
  });

  test("test_data_assignment_marks_modified", () => {
    const v = Entry.variable(1);
    v.data = 2;
    assert.ok(v.locally_modified);
  });

  test("test_in_place_mutation_not_observed", () => {
    const v = Entry.variable([1]);
    v.data.push(2);
    assert.equal(v.locally_modified, false);
  });

  t("test_bump_when_modified", () => {
    const v = Entry.variable(1);
    const old_ts = v.creation_timestamp;
    v.data = 2;

    time.sleep(0.002);
    assert.equal(v.bump_evolution_if_locally_modified(), true);
    assert.equal(v.evolution, 1);
    assert.notEqual(v.creation_timestamp, old_ts);
    // flag stays set until mark_memorized
    assert.ok(v.locally_modified);
  });

  test("test_bump_not_modified", () => {
    const v = Entry.variable(1);
    assert.equal(v.bump_evolution_if_locally_modified(), false);
    assert.equal(v.evolution, 0);
  });

  test("test_mark_memorized_resets", () => {
    const v = Entry.variable(1);
    v.data = 2;
    v.mark_memorized();
    assert.ok(!v.locally_modified);
    assert.equal(v.bump_evolution_if_locally_modified(), false);
  });

  test("test_bump_changes_hash", () => {
    const v = Entry.variable(1);
    const h = v.__hash__();
    v.data = 2;
    v.bump_evolution_if_locally_modified();
    assert.notEqual(v.__hash__(), h);
  });

  test("test_set_none_payload_marks_modified", () => {
    const v = Entry.variable(1);
    v.data = null;
    assert.equal(v.data, null);
    assert.ok(v.locally_modified);
  });

  test("test_assign_wrapped_compdata_passthrough", () => {
    const v = Entry.variable(1);
    const cd = new ComputationalData({ z: 1 });
    v.data = cd;
    assert.equal(v._payload, cd);
  });

  t("test_concurrent_evolve_consistent", () => {
    const v = Entry.variable(0);
    const out = [];

    const worker = () => {
      for (let i = 0; i < 200; i++) out.push(v.evolve(1).evolution);
    };

    const ts = T.range(8).map(() => new TH.Thread({ target: worker }));
    for (const th of ts) th.start();
    for (const th of ts) th.join();
    for (const th of ts) if (th.exception) throw th.exception;
    assert.deepEqual(new Set(out), new Set([1]));
  });
});

// ---------------------------------------------------------------------------
// State handling
// ---------------------------------------------------------------------------

describe("TestState", () => {
  test("test_state_setter", () => {
    const e = Entry.constant(1);
    e.state = EntryState.STALE;
    assert.equal(e.state, EntryState.STALE);
  });

  test("test_contingent_default_state_staged", () => {
    assert.equal(Entry.contingent({ data: 1 }).state, EntryState.STAGED);
  });

  test("test_contingent_explicit_state", () => {
    assert.equal(Entry.contingent({ data: 1, state: EntryState.READY }).state, EntryState.READY);
  });

  test("test_constitution_forces_staged", () => {
    const e = Entry.contingent({ constitution: ["def f(x): return x"], data: 1, state: EntryState.READY });
    assert.equal(e.state, EntryState.STAGED);
  });

  test("test_na_allowed_on_plain_entry", () => {
    // Documents: plain Entry accepts NA (only subclasses are pinned).
    const e = Entry.constant(1);
    e.state = EntryState.NA;
    assert.equal(e.state, EntryState.NA);
  });

  test("test_non_enum_state_rejected", () => {
    const e = Entry.constant(1);
    assert.throws(() => {
      e.state = "READY";
    });
  });

  test("test_entrystate_members", () => {
    assert.deepEqual(new Set([...EntryState].map((s) => s.name)), new Set(["READY", "POOLED", "POOLING", "STAGED", "STALE", "NA"]));
  });
});

// ---------------------------------------------------------------------------
// contingent / raw constructor
// ---------------------------------------------------------------------------

describe("TestContingent", () => {
  test("test_scopes_kwarg", () => {
    const e = Entry.contingent({ uuid: U1, scopes: ["X", "Y"], evolution: 2, data: 1 });
    assert.equal(e.global_id, `LAILA:X:Y:${U1}@evolution=2`);
  });

  test("test_payload_alias", () => {
    assert.deepEqual(Entry.contingent({ payload: [1] }).data, [1]);
  });

  test("test_global_id_kwarg_preserves_scopes", () => {
    const e = Entry.contingent({ global_id: `LAILA:CUSTOM:${U1}@evolution=4`, data: 1 });
    assert.deepEqual(e.scopes, ["CUSTOM"]);
    assert.equal(e.evolution, 4);
  });

  test("test_global_id_kwarg_overrides_uuid", () => {
    const e = Entry.contingent({ global_id: `LAILA:ENTRY:${U1}`, uuid: "ignored" });
    assert.equal(e.uuid, U1);
  });

  test("test_nickname_kwarg", () => {
    const e = Entry.contingent({ nickname: "c" });
    assert.equal(e.uuid, _LAILA_IDENTIFIABLE_OBJECT.generate_uuid_from_nickname("c"));
  });

  test("test_list_constitution_becomes_simple", () => {
    const e = Entry.contingent({ constitution: ["def f(x): return x"], data: 1 });
    assert.ok(e.constitution instanceof SimpleConstitution);
  });

  test("test_constitution_setter_none", () => {
    const e = Entry.contingent({ constitution: ["def f(x): return x"], data: 1 });
    e.constitution = null;
    assert.equal(e.constitution, null);
  });

  test("test_data_access_unbuilt_raises", () => {
    const e = Entry.contingent({ constitution: ["def f(x): return x"] });
    assert.throws(() => e.data, EntryNotBuiltError);
  });

  test("test_data_access_unbuilt_with_payload_returns_payload", () => {
    const e = Entry.contingent({ constitution: ["def f(x): return x"], data: 7 });
    assert.equal(e.data, 7);
  });
});

// ---------------------------------------------------------------------------
// as_dict / serialize / from_dict
// ---------------------------------------------------------------------------

describe("TestDictRoundTrip", () => {
  test("test_as_dict_keys", () => {
    const d = Entry.constant({ a: 1 }, { uuid: U1 }).as_dict();
    assert.deepEqual(new Set(Object.keys(d)), new Set(["_uuid", "_evolution", "_scopes", "_state", "_creation_timestamp", "payload", "constitution"]));
    assert.equal(d["_uuid"], U1);
    assert.equal(d["_evolution"], null);
    assert.deepEqual(d["_scopes"], ["ENTRY"]);
    assert.equal(d["_state"], "READY");
    assert.deepEqual(d["payload"], { a: 1 });
    assert.equal(d["constitution"], null);
  });

  test("test_as_dict_is_json_serializable", () => {
    pyjson.dumps(Entry.variable({ a: [1, 2] }).as_dict());
  });

  test("test_as_dict_scopes_copy", () => {
    const e = Entry.constant(1);
    const d = e.as_dict();
    d["_scopes"].push("X");
    assert.deepEqual(e.scopes, ["ENTRY"]);
  });

  test("test_from_dict_roundtrip_constant", () => {
    const e = Entry.constant({ a: 1 }, { uuid: U1 });
    const e2 = Entry.from_dict(e.as_dict());
    assert.equal(e2.global_id, e.global_id);
    assert.deepEqual(e2.data, { a: 1 });
    assert.equal(e2.state, EntryState.READY);
    assert.equal(e2.creation_timestamp, e.creation_timestamp);
    assert.equal(e2.locally_modified, false);
  });

  test("test_from_dict_roundtrip_variable", () => {
    const e = Entry.variable([1], { uuid: U1, evolution: 3 });
    const e2 = Entry.from_dict(e.as_dict());
    assert.equal(e2.evolution, 3);
    assert.equal(e2.global_id, e.global_id);
  });

  test("test_from_dict_custom_scopes", () => {
    const e = Entry.contingent({ uuid: U1, scopes: ["Q"], data: 1, state: EntryState.READY });
    assert.deepEqual(Entry.from_dict(e.as_dict()).scopes, ["Q"]);
  });

  test("test_from_dict_missing_timestamp_stamps_now", () => {
    const d = Entry.constant(1).as_dict();
    d["_creation_timestamp"] = null;
    assert.notEqual(Entry.from_dict(d).creation_timestamp, null);
  });

  test("test_from_dict_missing_state_defaults_staged", () => {
    const d = Entry.constant(1).as_dict();
    delete d["_state"];
    assert.equal(Entry.from_dict(d).state, EntryState.STAGED);
  });

  test("test_from_dict_missing_uuid_raises", () => {
    assert.throws(() => Entry.from_dict({}), E.KeyError);
  });

  test("test_serialize_non_ready_raises", () => {
    const e = Entry.contingent({ data: 1 }); // STAGED
    assert.throws(() => e.serialize(new TransformationSequence()), E.RuntimeError);
  });

  test("test_serialize_no_transformations_constant_returns_self", () => {
    const e = Entry.constant(1);
    assert.equal(e.serialize(null), e);
  });

  test("test_serialize_no_transformations_variable_returns_snapshot", () => {
    const e = Entry.variable([1]);
    const snap = e.serialize(null);
    assert.notEqual(snap, e);
    assert.equal(snap.global_id, e.global_id);
    assert.deepEqual(snap.data, [1]);
    assert.equal(snap.locally_modified, false);
  });

  test("test_snapshot_identity_independent", () => {
    const e = Entry.variable([1]);
    const snap = e.serialize(null);
    e.data = [2];
    e.bump_evolution_if_locally_modified();
    assert.equal(snap.evolution, 0);
    assert.deepEqual(snap.data, [1]);
  });

  test("test_snapshot_has_own_lock", () => {
    const e = Entry.variable(1);
    const snap = e.serialize(null);
    e.lock();
    try {
      assert.ok(snap.lock(0.1));
      snap.unlock();
    } finally {
      e.unlock();
    }
  });

  test("test_serialize_identity_pipeline", () => {
    const e = Entry.constant({ a: 1 }, { uuid: U1 });
    const d = e.serialize(new TransformationSequence());
    assert.ok(is_bytes(d["payload"]) || typeof d["payload"] === "string");
    assert.notEqual(d["constitution"], null);
    assert.equal(d["_uuid"], U1);
    const rebuilt = Entry._build_from_dict_sync(d);
    assert.deepEqual(rebuilt.data, { a: 1 });
    assert.equal(rebuilt.state, EntryState.READY);
  });

  test("test_serialize_base64_zlib_pipeline", () => {
    const e = Entry.constant(T.range(100));
    const d = e.serialize(new TransformationSequence({ transformations: [new Base64(), new Zlib()] }));
    assert.equal(typeof d["payload"], "string");
    assert.deepEqual(Entry._build_from_dict_sync(d).data, T.range(100));
  });

  test("test_serialize_json_roundtrip_through_text", () => {
    const e = Entry.variable({ k: "v" }, { uuid: U1 });
    const d = e.serialize(new TransformationSequence({ transformations: [new Base64()] }));
    const text = pyjson.dumps(d);
    const rebuilt = Entry._build_from_dict_sync(pyjson.loads(text));
    assert.deepEqual(rebuilt.data, { k: "v" });
    assert.equal(rebuilt.global_id, e.global_id);
  });

  test("test_serialize_none_payload", () => {
    const e = Entry.constant(null);
    const d = e.serialize(new TransformationSequence());
    assert.equal(d["payload"], null);
    const rebuilt = Entry._build_from_dict_sync(d);
    assert.equal(rebuilt.data, null);
  });

  test("test_serialize_numpy", () => {
    const arr = NDArray.arange(12, "<f4").reshape([3, 4]);
    const d = Entry.constant(arr).serialize(new TransformationSequence({ transformations: [new Base64()] }));
    const out = Entry._build_from_dict_sync(d).data;
    assert.ok(out instanceof NDArray);
    assert_array_equal(out, arr);
    assert.equal(out.dtype, "<f4");
  });

  test("test_serialize_nested_structures", () => {
    const val = { a: [1, { b: tuple([2, 3]) }], c: null, d: 1.5, e: true };
    const d = Entry.constant(val).serialize(new TransformationSequence());
    const out = Entry._build_from_dict_sync(d).data;
    assert.equal(out["a"][0], 1);
    assert.equal(out["c"], null);
    assert.equal(out["e"], true);
  });

  test("test_serialize_bytes_payload", () => {
    const d = Entry.constant(Buffer.from([0x00, 0xff])).serialize(new TransformationSequence({ transformations: [new Base64()] }));
    assert.ok(T.eq(Entry._build_from_dict_sync(d).data, Buffer.from([0x00, 0xff])));
  });

  test("test_serialize_unicode", () => {
    const d = Entry.constant("héllo ✓").serialize(new TransformationSequence({ transformations: [new Base64()] }));
    assert.equal(Entry._build_from_dict_sync(d).data, "héllo ✓");
  });

  t("test_build_from_dict_async", () => {
    const e = Entry.constant([1, 2]);
    const d = e.serialize(new TransformationSequence());
    const rebuilt = run(Entry._build_from_dict_async(d));
    assert.deepEqual(rebuilt.data, [1, 2]);
  });

  test("test_rebuilt_not_locally_modified", () => {
    const d = Entry.variable(1).serialize(new TransformationSequence());
    assert.equal(Entry._build_from_dict_sync(d).locally_modified, false);
  });

  test("test_from_dict_then_build_inplace", () => {
    const d = Entry.constant([9]).serialize(new TransformationSequence());
    const raw = Entry.from_dict(d);
    assert.notEqual(raw.constitution, null);
    raw._build_inplace();
    assert.deepEqual(raw.data, [9]);
    assert.equal(raw.constitution, null);
    assert.equal(raw.state, EntryState.READY);
  });

  test("test_str_repr", () => {
    const e = Entry.constant(1, { uuid: U1 });
    assert.ok(str(e).includes(U1));
    assert.ok(repr(e).includes(U1));
  });

  test("test_hash_in_dict", () => {
    const e = Entry.constant(1, { uuid: U1 });
    // ``{e: 1}``: JS collections key by reference; the hash string is the key.
    const d = new Map([[e.__hash__(), 1]]);
    assert.equal(d.get(e.__hash__()), 1);
  });
});

// ---------------------------------------------------------------------------
// Payload taxonomy via Entry
// ---------------------------------------------------------------------------

describe("TestPayloadTypes", () => {
  const cases = [
    ["int", 1],
    ["float", 1.5],
    ["str", "s"],
    ["bool", true],
    ["none", null],
    ["bytes", Buffer.from("b")],
    ["list", [1, 2]],
    ["tuple", tuple([1, 2])],
    ["dict", { a: 1 }],
    ["set", new Set([1, 2])],
    ["ndarray", NDArray.zeros([3])],
  ];
  for (const [id, val] of cases) {
    test(`test_accepts_many_types[${id}]`, () => {
      const e = Entry.constant(val);
      if (val === null) assert.equal(e.data, null);
      else if (val instanceof NDArray) assert_array_equal(e.data, val);
      else assert.ok(T.eq(e.data, val), `${repr(e.data)} != ${repr(val)}`);
    });
  }

  test("test_numpy_shape_via_metadata", () => {
    const e = Entry.constant(NDArray.zeros([2, 3]));
    assert.deepEqual([...e._payload.shape], [2, 3]);
  });

  test("test_dict_len", () => {
    assert.equal(T.len(Entry.constant({ a: 1, b: 2 })._payload), 2);
  });

  test("test_metadata_not_implemented", () => {
    assert.throws(() => Entry.constant([1, 2, 3]).metadata, E.NotImplementedError);
  });
});
