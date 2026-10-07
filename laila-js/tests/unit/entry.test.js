/**
 * Entry sub-package: ports of
 *   tests/functional/entry/unit_tests/test_entry.py
 *   tests/functional/entry/unit_tests/test_entry_torch_and_numpy.py (numpy half)
 *   tests/functional/entry/edge_cases/unit_tests/test_entry_edge_cases.py
 *   tests/functional/compdata/unit_tests/test_compdata.py (numpy half)
 *   tests/functional/compdata/edge_cases/unit_tests/test_compdata_edge_cases.py
 *   tests/functional/compdata/transformation/unit_tests/test_transformations.py
 *
 * Tests that need a running policy (``laila.build`` / ``laila.memorize``)
 * live in the functional tree once p5/p6 land.
 */
import { test, describe } from "node:test";
import assert from "node:assert/strict";
import crypto from "node:crypto";

const S = new URL("../../src/", import.meta.url).href;
const lazy_mod = await import(S + "_compat/lazy.js");
const { DotMap } = await import(S + "_compat/dotmap.js");
const { LAILA_UNIVERSAL_NAMESPACE } = await import(S + "macros/defaults.js");
const E = await import(S + "_compat/errors.js");
const T = await import(S + "_compat/pytypes.js");
const { repr } = await import(S + "_compat/pyrepr.js");
const str = (x) => (x && typeof x.__str__ === "function" ? x.__str__() : String(x));
const pyjson = await import(S + "_compat/pyjson.js");
const { NDArray } = await import(S + "_compat/ndarray.js");
const { Fernet } = await import(S + "_codecs/fernet.js");
const rc = await import(S + "_codecs/recovery_codes.js");
const { _LAILA_IDENTIFIABLE_OBJECT } = await import(S + "basics/definitions/identifiable_object.js");
const { fromisoformat: from_isoformat } = await import(S + "_compat/datetime.js");

// ``laila`` root is p8; the entry layer needs ``get_active_namespace`` and
// ``laila.args.encryption.key`` (read by ``resolve_encryption_key``).
const ARGS = new DotMap({ encryption: {} });
const LAILA_STUB = {
  get_active_namespace: () => LAILA_UNIVERSAL_NAMESPACE,
  args: ARGS,
  get encryption_key() {
    return ARGS.encryption.key;
  },
  set encryption_key(v) {
    ARGS.encryption.key = v;
  },
};
lazy_mod.register("laila", LAILA_STUB);

const ENT = await import(S + "entry/index.js");
const { Entry, EntryState, EntryNotBuiltError, ComputationalData, SimpleConstitution, TransformationSequence, Base64, Zlib, FernetEncryption } = ENT;
const { transformation_base64, transformation_base64_compression, transformation_base64_compression_encryption } = ENT;

const { PyTuple, PyFloat, PyByteArray, eq } = T;
const uuid4 = () => crypto.randomUUID();
const B = (s) => Buffer.from(s, "latin1");

// ---------------------------------------------------------------------------
// test_entry.py
// ---------------------------------------------------------------------------
describe("TestEntryFunctional", () => {
  const assert_ready = (e) => assert.equal(e.state, EntryState.READY);

  test("001 constant ready and data", () => {
    const e = Entry.constant([1, 2, 3]);
    assert_ready(e);
    assert.deepEqual(e.data, [1, 2, 3]);
    assert.equal(e.evolution, null);
  });

  test("002 variable ready and data", () => {
    const e = Entry.variable({ a: 1 });
    assert_ready(e);
    assert.deepEqual(e.data, { a: 1 });
  });

  test("003 entry accepts payload key", () => {
    const e = new Entry({ payload: [9, 8, 7], state: EntryState.READY });
    assert_ready(e);
    assert.deepEqual(e.data, [9, 8, 7]);
  });

  test("004 entry accepts data key", () => {
    const e = new Entry({ data: [4, 5, 6], state: EntryState.READY });
    assert_ready(e);
    assert.deepEqual(e.data, [4, 5, 6]);
  });

  test("005 constant rejects uuid and global_id together", () => {
    const gid = `LAILA:ENTRY:${uuid4()}`;
    assert.throws(() => Entry.constant(1, { uuid: uuid4(), global_id: gid }), E.RuntimeError);
  });

  test("006 constant rejects global_id with evolution", () => {
    const gid = `LAILA:ENTRY:${uuid4()}@evolution=1`;
    assert.throws(() => Entry.constant(1, { global_id: gid }), E.RuntimeError);
  });

  test("007 constant from global_id uses uuid and no evolution", () => {
    const raw_uuid = uuid4();
    const e = Entry.constant(123, { global_id: `LAILA:ENTRY:${raw_uuid}` });
    assert.equal(typeof e.uuid, "string");
    assert.equal(e.uuid, raw_uuid);
    assert.equal(e.evolution, null);
  });

  test("008 variable rejects global_id with uuid or evolution", () => {
    const gid = `LAILA:ENTRY:${uuid4()}`;
    assert.throws(() => Entry.variable(1, { global_id: gid, uuid: uuid4() }), E.RuntimeError);
    assert.throws(() => Entry.variable(1, { global_id: gid, evolution: 2 }), E.RuntimeError);
  });

  test("009 variable constitution requires manifest", () => {
    assert.throws(() => Entry.variable(null, { constitution: "def f(m):\n    return 0\n" }), E.ValueError);
  });

  test("010 evolve rejects constant", () => {
    const e = Entry.constant({ x: 1 });
    assert.throws(() => e.evolve({ x: 2 }), E.RuntimeError);
  });

  test("013 evolve on entry with evolution increments evolution", () => {
    const e = new Entry({ data: { x: 1 }, evolution: 0, state: EntryState.READY });
    const out = e.evolve({ x: 2 });
    assert.ok(out instanceof Entry);
    assert.notEqual(out, e);
    assert.equal(out.uuid, e.uuid);
    assert.equal(out.evolution, 1);
    assert.deepEqual(out.data, { x: 2 });
    assert.equal(out.state, EntryState.READY);
    assert.equal(e.evolution, 0);
    assert.deepEqual(e.data, { x: 1 });
  });

  test("014 global_id shape and scopes", () => {
    const e = Entry.constant(1);
    assert.ok(e.global_id.startsWith("LAILA:ENTRY:"));
    assert.ok(!e.global_id.includes("@"));
    assert.deepEqual(e.scopes, ["ENTRY"]);
  });

  test("015 str and repr return global_id", () => {
    const e = Entry.constant(1);
    assert.equal(str(e), e.global_id);
    assert.equal(repr(e), e.global_id);
    assert.equal(String(e), e.global_id);
  });

  test("016 serialize none protocol returns self", () => {
    const e = Entry.constant([1, 2, 3]);
    assert.equal(e.serialize(null), e);
    assert.equal(e.serialize(), e);
  });

  test("017 build entry is identity", () => {
    const e = Entry.constant([1]);
    assert.equal(Entry._build_from_dict_sync(e), e);
  });

  test("018 build invalid json raises", () => {
    assert.throws(() => Entry._build_from_dict_sync("{not valid json"), E.ValueError);
  });

  test("019 build from dict reconstructs entry", () => {
    const payload = {
      _uuid: uuid4(),
      _evolution: 3,
      _scopes: ["ENTRY"],
      _state: "READY",
      payload: null,
      constitution: { _kind: "simple", codes: [] },
    };
    const e = Entry._build_from_dict_sync(payload);
    assert.equal(typeof e.uuid, "string");
    assert.equal(e.evolution, 3);
    assert.equal(e.state, EntryState.READY);
    assert.equal(e.data, null);
  });

  test("020 build from json dict string", () => {
    const payload = {
      _uuid: uuid4(),
      _evolution: null,
      _scopes: ["ENTRY"],
      _state: "READY",
      payload: null,
      constitution: { _kind: "simple", codes: [] },
    };
    const e = Entry._build_from_dict_sync(pyjson.dumps(payload));
    assert.equal(typeof e.uuid, "string");
    assert.equal(e.evolution, null);
  });

  test("021 roundtrip with transformation_base64", () => {
    const original = { a: [1, 2, 3], b: { nested: true } };
    const e = Entry.constant(original);
    const serialized = e.serialize(transformation_base64);
    const recovered = Entry._build_from_dict_sync(serialized);
    assert.deepEqual(recovered.data, original);
    assert.equal(recovered.state, EntryState.READY);
  });

  test("022 roundtrip with transformation_base64_compression", () => {
    const original = { text: "abc123".repeat(100), nums: Array.from({ length: 100 }, (_, i) => i), meta: { ok: true } };
    const e = Entry.constant(original);
    const serialized = e.serialize(transformation_base64_compression);
    const recovered = Entry._build_from_dict_sync(serialized);
    assert.deepEqual(recovered.data, original);
    assert.equal(recovered.state, EntryState.READY);
  });

  test("023 constant nickname is deterministic", () => {
    const nickname = "unit:test:constant:nickname";
    const e1 = Entry.constant({ v: 1 }, { nickname });
    const e2 = Entry.constant({ v: 2 }, { nickname });
    const expected = Entry.generate_uuid_from_nickname(nickname);
    assert.equal(e1.uuid, expected);
    assert.equal(e2.uuid, expected);
    assert.equal(e1.global_id, e2.global_id);
    assert.equal(e1.evolution, null);
    assert.equal(e2.evolution, null);
  });

  test("024 constant nickname overrides uuid", () => {
    const nickname = "unit:test:constant:override";
    const explicit = uuid4();
    const e = Entry.constant(7, { uuid: explicit, nickname });
    const expected = Entry.generate_uuid_from_nickname(nickname);
    assert.notEqual(explicit, expected);
    assert.equal(e.uuid, expected);
    assert.equal(e.evolution, null);
  });

  test("025 variable nickname overrides uuid and sets evolution", () => {
    const nickname = "unit:test:variable:override";
    const explicit = uuid4();
    const e = Entry.variable({ x: 1 }, { uuid: explicit, nickname });
    const expected = Entry.generate_uuid_from_nickname(nickname);
    assert.notEqual(explicit, expected);
    assert.equal(e.uuid, expected);
    assert.equal(e.evolution, 0);
  });

  test("026 entry init nickname overrides uuid", () => {
    const nickname = "unit:test:entry:init:nickname";
    const explicit = uuid4();
    const e = new Entry({ data: { payload: true }, uuid: explicit, nickname, state: EntryState.READY });
    const expected = Entry.generate_uuid_from_nickname(nickname);
    assert.notEqual(explicit, expected);
    assert.equal(e.uuid, expected);
    assert.equal(e.state, EntryState.READY);
  });
});

// ---------------------------------------------------------------------------
// test_entry_edge_cases.py
// ---------------------------------------------------------------------------
describe("TestEntryCreationTimestamp", () => {
  test("constant is stamped", () => {
    const e = Entry.constant(1);
    from_isoformat(e.creation_timestamp);
  });

  test("as_dict carries stamp", () => {
    const e = Entry.constant(1);
    assert.equal(e.as_dict()._creation_timestamp, e.creation_timestamp);
  });

  test("serialize roundtrip preserves stamp", () => {
    const e = Entry.constant({ a: [1, 2, 3] });
    const blob = e.serialize(transformation_base64);
    assert.equal(blob._creation_timestamp, e.creation_timestamp);
    const rebuilt = Entry._build_from_dict_sync(pyjson.dumps(blob));
    assert.equal(rebuilt.creation_timestamp, e.creation_timestamp);
    assert.deepEqual(rebuilt.data, { a: [1, 2, 3] });
  });

  test("from_dict without stamp falls back to now", () => {
    const e = Entry.constant(1);
    const blob = e.serialize(transformation_base64);
    delete blob._creation_timestamp;
    const rebuilt = Entry._build_from_dict_sync(blob);
    assert.notEqual(rebuilt.creation_timestamp, null);
    from_isoformat(rebuilt.creation_timestamp);
  });

  test("evolve gets fresh stamp", () => {
    const v = Entry.variable(1);
    v._creation_timestamp = "2000-01-01T00:00:00.000+00:00";
    const v2 = v.evolve(2);
    assert.notEqual(v2.creation_timestamp, v.creation_timestamp);
    assert.ok(v2.creation_timestamp > v.creation_timestamp);
  });
});

describe("TestEntryConstantEdgeCases", () => {
  test("constant string data", () => {
    const e = Entry.constant("hello");
    assert.equal(e.data, "hello");
    assert.equal(e.evolution, null);
  });
  test("constant dict data", () => assert.deepEqual(Entry.constant({ key: "val" }).data, { key: "val" }));
  test("constant list data", () => assert.deepEqual(Entry.constant([1, 2, 3]).data, [1, 2, 3]));
  test("constant int data", () => assert.equal(Entry.constant(42).data, 42));
  test("constant float data", () => assert.ok(Math.abs(Entry.constant(3.14).data - 3.14) < 1e-9));
  test("constant boolean data", () => assert.equal(Entry.constant(true).data, true));
  test("constant empty string", () => assert.equal(Entry.constant("").data, ""));
  test("constant empty dict", () => assert.deepEqual(Entry.constant({}).data, {}));
  test("constant empty list", () => assert.deepEqual(Entry.constant([]).data, []));

  test("constant with nickname produces deterministic uuid", () => {
    const a = Entry.constant("x", { nickname: "same-nick" });
    const b = Entry.constant("y", { nickname: "same-nick" });
    assert.equal(a.uuid, b.uuid);
  });
  test("constant different nicknames differ", () => {
    const a = Entry.constant("x", { nickname: "nick-a" });
    const b = Entry.constant("x", { nickname: "nick-b" });
    assert.notEqual(a.uuid, b.uuid);
  });
  test("constant global_id and uuid raises", () => {
    const uid = uuid4();
    assert.throws(() => Entry.constant("x", { global_id: `LAILA:ENTRY:${uid}`, uuid: uid }), E.RuntimeError);
  });
  test("constant has no evolution", () => assert.equal(Entry.constant("x").evolution, null));
  test("constant state is ready", () => assert.equal(Entry.constant("x").state, EntryState.READY));
});

describe("TestEntryVariableEdgeCases", () => {
  test("variable has evolution zero", () => assert.equal(Entry.variable("x").evolution, 0));
  test("variable state is ready", () => assert.equal(Entry.variable("x").state, EntryState.READY));
  test("variable with explicit evolution", () => assert.equal(Entry.variable("x", { evolution: 5 }).evolution, 5));
  test("variable global_id and uuid raises", () => {
    const uid = uuid4();
    assert.throws(() => Entry.variable("x", { global_id: `LAILA:ENTRY:${uid}@evolution=0`, uuid: uid }), E.RuntimeError);
  });
  test("variable constitution requires manifest", () => {
    assert.throws(() => Entry.variable(null, { constitution: "def f(m):\n    return 0\n" }), E.ValueError);
  });
  test("variable from global_id with evolution", () => {
    const uid = uuid4();
    const e = Entry.variable("x", { global_id: `LAILA:ENTRY:${uid}@evolution=4` });
    assert.equal(e.uuid, uid);
    assert.equal(e.evolution, 4);
  });
});

describe("TestEntryEvolve", () => {
  test("evolve increments evolution", () => {
    const e = Entry.variable("v0");
    assert.equal(e.evolution, 0);
    const e1 = e.evolve("v1");
    assert.equal(e1.evolution, 1);
    const e2 = e1.evolve("v2");
    assert.equal(e2.evolution, 2);
    assert.equal(e.evolution, 0);
    assert.equal(e1.uuid, e.uuid);
    assert.equal(e2.uuid, e.uuid);
  });
  test("evolve updates data", () => {
    const e = Entry.variable("old");
    const e2 = e.evolve("new");
    assert.equal(e2.data, "new");
    assert.equal(e.data, "old");
  });
  test("evolve constant raises", () => {
    assert.throws(() => Entry.constant("fixed").evolve("changed"), E.RuntimeError);
  });
  test("evolve rejects constitution kwarg", () => {
    const e = Entry.variable("x");
    assert.throws(() => e.evolve("y", { constitution: "def f(m):\n    return 0\n" }), E.TypeError);
  });
  test("evolve with none data", () => {
    const e = Entry.variable("start");
    const e2 = e.evolve(null);
    assert.equal(e2.data, null);
    assert.equal(e2.evolution, 1);
    assert.equal(e.data, "start");
    assert.equal(e.evolution, 0);
  });
});

describe("TestEntryLocallyModified", () => {
  test("fresh variable is not locally modified; data setter marks it", () => {
    const v = Entry.variable({ x: 1 });
    assert.equal(v.locally_modified, false);
    v.data = { x: 2 };
    assert.equal(v.locally_modified, true);
    assert.deepEqual(v.data, { x: 2 });
    assert.equal(v.state, EntryState.READY);
  });
  test("bump_evolution_if_locally_modified increments and refreshes stamp", () => {
    const v = Entry.variable({ x: 1 });
    assert.equal(v.bump_evolution_if_locally_modified(), false);
    v.data = { x: 2 };
    v._creation_timestamp = "2000-01-01T00:00:00.000+00:00";
    assert.equal(v.bump_evolution_if_locally_modified(), true);
    assert.equal(v.evolution, 1);
    assert.ok(v.global_id.endsWith("@evolution=1"));
    assert.ok(v.creation_timestamp > "2000-01-02");
    // the flag is only cleared by ``mark_memorized`` (central memory, after a write)
    assert.equal(v.locally_modified, true);
    v.mark_memorized();
    assert.equal(v.locally_modified, false);
    assert.equal(v.bump_evolution_if_locally_modified(), false);
    assert.equal(v.evolution, 1);
  });
  test("mark_memorized clears flag only", () => {
    const v = Entry.variable({ x: 1 });
    v.data = 3;
    v.mark_memorized();
    assert.equal(v.locally_modified, false);
    assert.equal(v.state, EntryState.READY);
  });
  test("serialize(null) snapshots variables and returns constants", () => {
    const c = Entry.constant(1);
    assert.equal(c.serialize(null), c);
    const v = Entry.variable([1]);
    const snap = v.serialize(null);
    assert.notEqual(snap, v);
    assert.equal(snap.global_id, v.global_id);
    assert.deepEqual(snap.data, [1]);
    v.data = [2];
    v.bump_evolution_if_locally_modified();
    assert.equal(snap.evolution, 0);
    assert.equal(v.evolution, 1);
  });
  test("constant data setter does not bump (no evolution)", () => {
    const c = Entry.constant(1);
    c.data = 2;
    assert.equal(c.data, 2);
    assert.equal(c.bump_evolution_if_locally_modified(), false);
    assert.equal(c.evolution, null);
  });
});

describe("TestEntrySerialize", () => {
  test("serialize none transformations returns entry", () => {
    const e = Entry.constant("x");
    const result = e.serialize(null);
    assert.ok(result instanceof Entry);
    assert.equal(result, e);
  });
  test("serialize with transformations returns dict", () => {
    const e = Entry.constant("hello");
    const ts = new TransformationSequence({ transformations: [new Base64()] });
    const result = e.serialize(ts);
    assert.ok(T.isdict(result));
    assert.ok("payload" in result);
    assert.ok("constitution" in result);
    assert.equal(result.constitution._kind, "simple");
  });
  test("serialize preserves global_id info", () => {
    const e = Entry.constant("x", { nickname: "ser-test" });
    const result = e.serialize(new TransformationSequence({ transformations: [new Base64()] }));
    assert.ok("_uuid" in result);
    assert.equal(result._uuid, e.uuid);
  });
  test("serialized dict has Python key order", () => {
    const e = Entry.variable("x");
    const d = e.serialize(transformation_base64);
    assert.deepEqual(Object.keys(d), ["_uuid", "_evolution", "_scopes", "_state", "_creation_timestamp", "payload", "constitution"]);
    assert.equal(d._state, "READY");
    assert.equal(d._evolution, 0);
    assert.equal(d.constitution._kind, "simple");
    assert.equal(d.constitution.codes.length, 2); // base64 backward + pickle backward
  });
});

describe("TestEntryBuild", () => {
  test("build from entry passthrough", () => {
    const e = Entry.constant("x");
    assert.equal(Entry._build_from_dict_sync(e), e);
  });
  test("build from invalid string raises", () => {
    assert.throws(() => Entry._build_from_dict_sync("not json"), E.ValueError);
  });
  test("build from empty dict raises", () => {
    assert.throws(() => Entry._build_from_dict_sync({}), E.KeyError);
  });
  test("build invalid state raises", () => {
    const d = {
      _uuid: "00000000-0000-0000-0000-000000000000",
      _evolution: null,
      _scopes: ["ENTRY"],
      _state: "NONEXISTENT_STATE",
      payload: null,
      constitution: { _kind: "simple", codes: [] },
    };
    assert.throws(() => Entry._build_from_dict_sync(d), E.KeyError);
  });
  test("serialize build round trip", () => {
    const e = Entry.constant("round-trip-data");
    const serialized = e.serialize(new TransformationSequence({ transformations: [new Base64()] }));
    assert.equal(Entry._build_from_dict_sync(serialized).data, "round-trip-data");
  });
  test("serialize non ready raises", () => {
    const e = Entry.constant("x");
    e._state = EntryState.STAGED;
    assert.throws(() => e.serialize(new TransformationSequence({ transformations: [new Base64()] })), E.RuntimeError);
  });
  test("build_from_dict async variant", async () => {
    const e = Entry.constant({ k: [1, 2] });
    const serialized = e.serialize(transformation_base64_compression);
    const r = await Entry._build_from_dict_async(serialized);
    assert.deepEqual(r.data, { k: [1, 2] });
    assert.equal(r.global_id, e.global_id);
  });
  test("metadata is not implemented (as in Python)", () => {
    assert.throws(() => Entry.variable([1]).metadata, E.NotImplementedError);
  });
});

describe("TestEntryGlobalId", () => {
  test("constant global_id has no evolution suffix", () => assert.ok(!Entry.constant("x").global_id.includes("@")));
  test("variable global_id has evolution suffix", () => assert.ok(Entry.variable("x").global_id.endsWith("@evolution=0")));
  test("global_id is valid laila resource", () => assert.ok(_LAILA_IDENTIFIABLE_OBJECT.is_laila_resource(Entry.constant("x").global_id)));
  test("str is global_id", () => {
    const e = Entry.constant("x");
    assert.equal(str(e), e.global_id);
  });
  test("repr is global_id", () => {
    const e = Entry.constant("x");
    assert.equal(repr(e), e.global_id);
  });
});

// ---------------------------------------------------------------------------
// test_entry_torch_and_numpy.py (NumPy half; torch is unavailable in JS)
// ---------------------------------------------------------------------------
describe("TestEntryTensors", () => {
  const TEST_FERNET_KEY = Fernet.generate_key();
  LAILA_STUB.encryption_key = TEST_FERNET_KEY;

  const assert_np_equal = (got, expected) => {
    assert.ok(got instanceof NDArray);
    assert.deepEqual([...got.shape], [...expected.shape]);
    assert.equal(got.dtype, expected.dtype);
    assert.ok(got.array_equal(expected, { equal_nan: true }), `${repr(got)} != ${repr(expected)}`);
  };
  const rt = (arr, preset) => Entry._build_from_dict_sync(Entry.constant(arr).serialize(preset));

  test("001 numpy b64 int32 1d", () => {
    const arr = NDArray.array([1, 2, 3], "<i4");
    assert_np_equal(rt(arr, transformation_base64).data, arr);
  });
  test("002 numpy b64 zlib float64 2d", () => {
    const arr = NDArray.arange(12, "<f8").reshape([3, 4]);
    assert_np_equal(rt(arr, transformation_base64_compression).data, arr);
  });
  test("003 numpy b64 zlib fernet int16 3d", () => {
    const arr = NDArray.arange(24, "<i2").reshape([2, 3, 4]);
    assert_np_equal(rt(arr, transformation_base64_compression_encryption(TEST_FERNET_KEY)).data, arr);
  });
  test("004 numpy empty float32", () => {
    const arr = new NDArray({ dtype: "<f4", shape: [0], data: [] });
    assert_np_equal(rt(arr, transformation_base64).data, arr);
  });
  test("005 numpy fortran order float32", () => {
    const arr = new NDArray({ dtype: "<f4", shape: [3, 3], data: Array.from({ length: 9 }, (_, i) => i), fortran_order: true });
    const r = rt(arr, transformation_base64_compression).data;
    assert_np_equal(r, arr);
    assert.deepEqual(r.tolist(), arr.tolist());
  });
  test("006 numpy bool 1d", () => {
    const arr = NDArray.array([true, false, true]);
    assert.equal(arr.dtype, "|b1");
    assert_np_equal(rt(arr, transformation_base64_compression_encryption(TEST_FERNET_KEY)).data, arr);
  });
  test("007 numpy dtype shape uint8", () => {
    const arr = NDArray.arange(5, "|u1");
    const r = rt(arr, transformation_base64).data;
    assert.equal(r.dtype, "|u1");
    assert.deepEqual([...r.shape], [5]);
  });
  test("015 numpy all presets smoke", () => {
    const arr = new NDArray({ dtype: "<f8", shape: [17, 1], data: Array.from({ length: 17 }, (_, i) => i / 16) });
    assert_np_equal(rt(arr, transformation_base64).data, arr);
    assert_np_equal(rt(arr, transformation_base64_compression).data, arr);
    assert_np_equal(rt(arr, transformation_base64_compression_encryption(TEST_FERNET_KEY)).data, arr);
  });
  test("numpy payload uses the numpy serializer (npy magic)", () => {
    const cd = new ComputationalData(NDArray.arange(3));
    assert.equal(cd.constructor.name, "CD_numpyarray");
    const [payload, code] = cd.serialize();
    assert.ok(Buffer.isBuffer(payload) || payload instanceof Uint8Array);
    assert.equal(Buffer.from(payload).subarray(0, 6).toString("latin1"), "\x93NUMPY");
    assert.ok(rc.recognize(code) !== null);
  });
});

// ---------------------------------------------------------------------------
// test_compdata_edge_cases.py
// ---------------------------------------------------------------------------
function _build_simple(payload, codes) {
  const result = new SimpleConstitution({ codes }).build(payload);
  if (result !== null && result !== undefined && !(result instanceof ComputationalData)) return new ComputationalData(result);
  return result;
}

describe("TestComputationalDataFactory", () => {
  test("string data", () => assert.equal(new ComputationalData("hello").data, "hello"));
  test("int data", () => assert.equal(new ComputationalData(42).data, 42));
  test("float data", () => assert.ok(Math.abs(new ComputationalData(3.14).data - 3.14) < 1e-9));
  test("dict data", () => assert.deepEqual(new ComputationalData({ key: "value" }).data, { key: "value" }));
  test("list data", () => assert.deepEqual(new ComputationalData([1, 2, 3]).data, [1, 2, 3]));
  test("bool data", () => assert.equal(new ComputationalData(true).data, true));
  test("bytes data", () => assert.ok(new ComputationalData(B("binary")).data.equals(B("binary"))));
  test("tuple data", () => {
    const t = PyTuple.from_iterable([1, 2, 3]);
    assert.ok(eq(new ComputationalData(t).data, t));
  });
  test("nested dict", () => {
    const d = { a: { b: { c: [1, 2] } } };
    assert.deepEqual(new ComputationalData(d).data, d);
  });
  test("none data raises", () => assert.throws(() => new ComputationalData(null), E.TypeError));
  test("no args raises", () => assert.throws(() => new ComputationalData(), E.TypeError));
  test("two positional args raises", () => assert.throws(() => new ComputationalData(1, 2), E.TypeError));
  test("positional and keyword raises", () => assert.throws(() => new ComputationalData(1, { data: 2 }), E.TypeError));
  test("keyword data", () => assert.equal(new ComputationalData({ data: "kw" }).data.data ?? new ComputationalData({ data: "kw" }).data, "kw"));
});

describe("TestComputationalDataSerialize", () => {
  for (const [name, v] of [
    ["string", "test"],
    ["dict", { a: 1 }],
    ["int", 42],
    ["list", [1, 2, 3]],
    ["empty string", ""],
  ]) {
    test(`serialize ${name}`, () => {
      const [payload, code] = new ComputationalData(v).serialize();
      assert.notEqual(payload, null);
      assert.equal(typeof code, "string");
    });
  }
});

describe("TestComputationalDataRecover", () => {
  const roundtrip = (v) => {
    const [payload, code] = new ComputationalData(v).serialize();
    return _build_simple(payload, [code]).data;
  };
  test("round trip string", () => assert.equal(roundtrip("hello"), "hello"));
  test("round trip dict", () => assert.deepEqual(roundtrip({ k: [1, 2] }), { k: [1, 2] }));
  test("round trip int", () => assert.equal(roundtrip(42), 42));
  test("round trip list", () => assert.ok(eq(roundtrip([1, "two", new PyFloat(3.0)]), [1, "two", new PyFloat(3.0)])));
  test("round trip bool", () => assert.equal(roundtrip(false), false));
  test("recover empty sequence", () => {
    const result = _build_simple("raw-data", []);
    assert.ok(result instanceof ComputationalData);
    assert.equal(result.data, "raw-data");
  });
  test("recover none payload empty sequence", () => assert.equal(_build_simple(null, []), null));
  test("recover bad code raises", () => assert.throws(() => _build_simple(B("data"), ["this is not valid python"])));
  test("recover code with zero callables raises", () => assert.throws(() => _build_simple(B("data"), ["x = 42"]), E.ValueError));
  test("recover code with two callables raises", () => {
    assert.throws(() => _build_simple(B("data"), ["def a(x): return x\ndef b(x): return x"]), E.ValueError);
  });
});

describe("TestComputationalDataMisc", () => {
  test("repr", () => assert.ok(repr(new ComputationalData("test")).includes("test")));
  test("str", () => assert.ok(str(new ComputationalData(42)).includes("42")));
  test("getitem", () => {
    const cd = new ComputationalData([10, 20, 30]);
    assert.equal(cd[0], 10);
    assert.equal(cd[2], 30);
    assert.equal(cd.__getitem__(2), 30);
  });
  test("getitem dict", () => assert.equal(new ComputationalData({ a: 1, b: 2 })["a"], 1));
  test("getitem out of range raises", () => assert.throws(() => new ComputationalData([1]).__getitem__(10), E.IndexError));
  test("getitem bad key raises", () => assert.throws(() => new ComputationalData({ a: 1 }).__getitem__("z"), E.KeyError));
  test("serializer property", () => assert.notEqual(new ComputationalData("x").serializer, null));
  test("serializer setter wrong type raises", () => {
    const cd = new ComputationalData("x");
    assert.throws(() => {
      cd.serializer = "not-a-serializer";
    }, E.TypeError);
  });
  test("large data round trip", () => {
    const big = Array.from({ length: 10_000 }, (_, i) => i);
    const [payload, code] = new ComputationalData(big).serialize();
    assert.deepEqual(_build_simple(payload, [code]).data, big);
  });
});

// ---------------------------------------------------------------------------
// test_compdata.py (numpy half)
// ---------------------------------------------------------------------------
describe("TestComputationalDataFunctional", () => {
  const roundtrip = (cd) => {
    const [payload, code] = cd.serialize();
    const out = _build_simple(payload, [code]);
    return out instanceof ComputationalData ? out.data : out;
  };
  const roundtrip_chain = (cd) => {
    const [payload, code] = cd.serialize();
    const ts = new TransformationSequence({ transformations: [new Base64(), new Zlib()] });
    const [transformed, inverse] = ts.forward(payload);
    const out = _build_simple(transformed, [...inverse, code]);
    return out instanceof ComputationalData ? out.data : out;
  };
  const assert_np_equal = (recovered, original) => {
    assert.ok(recovered instanceof NDArray);
    assert.deepEqual([...recovered.shape], [...original.shape]);
    assert.equal(recovered.dtype, original.dtype);
    assert.ok(recovered.array_equal(original, { equal_nan: true }), `${repr(recovered)} != ${repr(original)}`);
  };

  test("001 init variants", () => {
    const a = new ComputationalData(NDArray.array([1, 2]), {});
    const b = new ComputationalData(NDArray.array([3, 4]));
    assert.deepEqual([...a.data.shape], [2]);
    assert.deepEqual([...b.data.shape], [2]);
  });
  test("002 conflicting init", () => {
    assert.throws(() => new ComputationalData(NDArray.array([1, 2]), { data: NDArray.array([3, 4]) }), E.TypeError);
  });
  test("003 factory numpy", () => {
    const a = new ComputationalData(NDArray.array([1, 2, 3]));
    assert.ok(a.constructor.name.includes("CD_numpyarray"));
    assert.equal(a.__len__(), 3);
    assert.ok(eq(a.shape, PyTuple.from_iterable([3])));
  });
  test("004 factory list", () => {
    const a = new ComputationalData([1, 2, 3, 4]);
    assert.ok(a.constructor.name.includes("CD_list"));
    assert.equal(a.__len__(), 4);
    assert.ok(eq(a.shape, PyTuple.from_iterable([4])));
  });
  test("005 factory tuple", () => {
    const a = new ComputationalData(PyTuple.from_iterable([1, 2]));
    assert.ok(a.constructor.name.includes("CD_list"));
    assert.equal(a.__len__(), 2);
  });
  test("006 factory dict", () => {
    const a = new ComputationalData({ x: 1 });
    assert.ok(a.constructor.name.includes("CD_dict"));
    assert.equal(a.__len__(), 1);
    assert.throws(() => a.shape, E.AttributeError);
  });
  test("007 factory fallback generic for set", () => {
    const a = new ComputationalData(new Set([1, 2, 3]));
    assert.ok(a.constructor.name.includes("CD_generic"));
    assert.equal(a.__len__(), 3);
  });
  test("008 serializer roundtrip no chain", () => {
    const arr = NDArray.array([1, 2, 3, 4]);
    const [payload, code] = new ComputationalData(arr).serialize();
    assert.ok(payload instanceof Uint8Array);
    assert.equal(typeof code, "string");
    const b = _build_simple(payload, [code]);
    assert.ok(b instanceof ComputationalData);
    assert_np_equal(b.data, arr);
  });
  test("009 transform chain base64 zlib", () => {
    const arr = NDArray.array([1, 2, 3, 4]);
    const [payload, code] = new ComputationalData(arr).serialize();
    const ts = new TransformationSequence({ transformations: [new Base64(), new Zlib()] });
    const [transformed, inverse] = ts.forward(payload);
    assert.equal(typeof transformed, "string");
    assert.equal(inverse.length, 2);
    const recovered = _build_simple(transformed, [...inverse, code]);
    assert.ok(recovered instanceof ComputationalData);
    assert_np_equal(recovered.data, arr);
  });
  test("010 serializer get/set", () => {
    const a = new ComputationalData(NDArray.array([0]));
    assert.notEqual(a.serializer, null);
    const old = a.serializer;
    a.serializer = new old.constructor();
    assert.ok(a.serializer instanceof old.constructor);
    assert.throws(() => {
      a.serializer = new Base64();
    }, E.TypeError);
  });
  test("011 getitem proxy", () => assert.equal(new ComputationalData([10, 20, 30])[1], 20));
  test("012 repr", () => {
    const a = new ComputationalData({ k: 9 });
    const r = repr(a);
    assert.ok(r.includes(a.constructor.name));
    assert.ok(r.includes("'k': 9"));
  });
  test("013 scalar int roundtrip", () => {
    const a = new ComputationalData(12345);
    assert.ok(a.constructor.name.includes("CD_generic"));
    assert.throws(() => a.__len__(), E.TypeError);
    const [payload, code] = a.serialize();
    assert.ok(payload instanceof Uint8Array);
    assert.equal(_build_simple(payload, [code]).data, 12345);
  });
  test("014 scalar float chain roundtrip", () => {
    const a = new ComputationalData(3.14159);
    const [payload, code] = a.serialize();
    const ts = new TransformationSequence({ transformations: [new Base64(), new Zlib()] });
    const [transformed, inverse] = ts.forward(payload);
    assert.equal(typeof transformed, "string");
    assert.equal(_build_simple(transformed, [...inverse, code]).data, 3.14159);
  });
  test("018 object without len", () => {
    class NoLen {}
    const a = new ComputationalData(new NoLen());
    assert.ok(a.constructor.name.includes("CD_generic"));
    assert.throws(() => a.__len__(), E.TypeError);
  });

  // --- dtype x shape x mode matrix ---------------------------------------
  const NP_DTYPES = ["<i1", "<i2", "<i4", "<i8", "|u1", "<u2", "<u4", "<u8", "<f2", "<f4", "<f8", "|b1", "<c8", "<c16"];
  const SHAPES = [[], [0], [1], [5], [3, 4], [2, 3, 4]];
  const kind = (dt) => dt.replace(/^[<>|=]/, "")[0];
  const gen_np = (dtype, shape) => {
    const size = shape.length ? shape.reduce((a, b) => a * b, 1) : 1;
    const k = kind(dtype);
    let flat;
    if (k === "b") flat = Array.from({ length: size }, (_, i) => i % 2 === 0);
    else if (k === "i" || k === "u") flat = Array.from({ length: size }, (_, i) => i);
    else if (k === "f") flat = Array.from({ length: size }, (_, i) => (size === 1 ? -1 : -1 + (2 * i) / (size - 1)));
    else if (k === "c") flat = Array.from({ length: size * 2 }, (_, j) => (j % 2 === 0 ? -1 + (2 * (j >> 1)) / Math.max(size - 1, 1) : 0.5 - (j >> 1) / Math.max(size - 1, 1)));
    else flat = new Array(size).fill(0);
    return new NDArray({ dtype, shape, data: flat });
  };
  const shape_tag = (s) => (s.length === 0 ? "scalar" : s.join("x"));

  for (const dt of NP_DTYPES) {
    for (const sh of SHAPES) {
      for (const mode of ["raw", "chain"]) {
        test(`np ${dt} ${shape_tag(sh)} ${mode}`, () => {
          const arr = gen_np(dt, sh);
          const cd = new ComputationalData(arr);
          const recovered = mode === "raw" ? roundtrip(cd) : roundtrip_chain(cd);
          assert.equal(recovered.dtype, arr.dtype, `dtype drift for ${dt} shape=${sh} mode=${mode}`);
          assert_np_equal(recovered, arr);
        });
      }
    }
  }

  // --- edge cases -----------------------------------------------------------
  test("np edge fortran order", () => {
    const arr = new NDArray({ dtype: "<f8", shape: [3, 4], data: Array.from({ length: 12 }, (_, i) => i), fortran_order: true });
    const recovered = roundtrip(new ComputationalData(arr));
    assert.equal(recovered.dtype, arr.dtype);
    assert_np_equal(recovered, arr);
  });
  test("np edge big endian i4", () => {
    const arr = NDArray.arange(5, "<i4").astype(">i4");
    const recovered = roundtrip(new ComputationalData(arr));
    assert.equal(recovered.dtype, ">i4");
    assert_np_equal(recovered, arr);
  });
  test("np edge big endian f8", () => {
    const arr = new NDArray({ dtype: "<f8", shape: [6], data: [-1, -0.6, -0.2, 0.2, 0.6, 1] }).astype(">f8");
    const recovered = roundtrip(new ComputationalData(arr));
    assert.equal(recovered.dtype, ">f8");
    assert_np_equal(recovered, arr);
  });
  for (const [name, dt] of [
    ["float16", "<f2"],
    ["float32", "<f4"],
    ["float64", "<f8"],
  ]) {
    test(`np edge nan/inf ${name}`, () => {
      const arr = new NDArray({ dtype: dt, shape: [5], data: [1, NaN, -1, Infinity, -Infinity] });
      const recovered = roundtrip(new ComputationalData(arr));
      assert.equal(recovered.dtype, dt);
      assert_np_equal(recovered, arr);
    });
  }
  test("np edge subnormal float64", () => {
    const tiny = 2.2250738585072014e-308;
    const arr = new NDArray({ dtype: "<f8", shape: [3], data: [tiny / 2, tiny, -tiny / 2] });
    assert_np_equal(roundtrip(new ComputationalData(arr)), arr);
  });
  for (const [name, dt, lo, hi] of [
    ["int8", "|i1", -128, 127],
    ["int16", "<i2", -32768, 32767],
    ["int32", "<i4", -2147483648, 2147483647],
    ["int64", "<i8", -(2n ** 63n), 2n ** 63n - 1n],
    ["uint8", "|u1", 0, 255],
    ["uint64", "<u8", 0n, 2n ** 64n - 1n],
  ]) {
    test(`np edge min max ${name}`, () => {
      const arr = new NDArray({ dtype: dt, shape: [3], data: [lo, typeof lo === "bigint" ? 0n : 0, hi] });
      const recovered = roundtrip(new ComputationalData(arr));
      assert.equal(recovered.dtype, dt);
      assert_np_equal(recovered, arr);
    });
  }
  test("np edge complex with imag", () => {
    const arr = new NDArray({ dtype: "<c8", shape: [3], data: [1, 2, -3, 4, 0, -1] });
    const recovered = roundtrip(new ComputationalData(arr));
    assert.equal(recovered.dtype, "<c8");
    assert_np_equal(recovered, arr);
  });
  test("np edge large 1d float32", () => {
    const n = 100_000;
    const arr = new NDArray({ dtype: "<f4", shape: [n], data: Float32Array.from({ length: n }, (_, i) => -1 + (2 * i) / (n - 1)) });
    const recovered = roundtrip(new ComputationalData(arr));
    assert.equal(recovered.dtype, "<f4");
    assert_np_equal(recovered, arr);
  });
});

// ---------------------------------------------------------------------------
// test_transformations.py
// ---------------------------------------------------------------------------
describe("TestBase64", () => {
  const op = new Base64();
  test("forward known bytes", () => assert.equal(op.forward(B("hello")), "aGVsbG8="));
  test("backward known string", () => assert.ok(op.backward("aGVsbG8=").equals(B("hello"))));
  test("round trip", () => {
    const data = Buffer.from([0, 0xff, ...B("binary")]);
    assert.ok(op.backward(op.forward(data)).equals(data));
  });
  test("forward non bytes raises", () => assert.throws(() => op.forward("not-bytes"), E.TypeError));
  test("forward empty bytes", () => assert.equal(op.forward(B("")), ""));
  test("backward empty string", () => assert.equal(op.backward("").length, 0));
  test("forward bytearray", () => assert.equal(op.forward(new PyByteArray(B("hello"))), "aGVsbG8="));
  test("backward invalid base64 raises", () => assert.throws(() => op.backward("!!!not-base64!!!"), E.ValueError));
  test("backward bytes input", () => assert.ok(op.backward(B("aGVsbG8=")).equals(B("hello"))));
  test("round trip large payload", () => {
    const data = crypto.randomBytes(100_000);
    assert.ok(op.backward(op.forward(data)).equals(data));
  });
  test("forward int raises", () => assert.throws(() => op.forward(42), E.TypeError));
  test("forward none raises", () => assert.throws(() => op.forward(null), E.TypeError));
  test("backward_code is executable", () => {
    const fn = rc.compile_backward(op.backward_code);
    assert.ok(fn("aGVsbG8=").equals(B("hello")));
    assert.ok(op.backward_code.includes("def "));
  });
});

describe("TestZlib", () => {
  const op = new Zlib();
  test("round trip", () => {
    const text = "unicode: \u2603 and ascii";
    assert.equal(op.backward(op.forward(text)), text);
  });
  test("forward non string raises", () => assert.throws(() => op.forward(B("bytes")), E.TypeError));
  test("forward empty string", () => assert.equal(op.backward(op.forward("")), ""));
  test("round trip large text", () => {
    const text = "x".repeat(100_000);
    assert.equal(op.backward(op.forward(text)), text);
  });
  test("forward none raises", () => assert.throws(() => op.forward(null), E.TypeError));
  test("backward invalid base64 raises", () => assert.throws(() => op.backward("!!!not-valid!!!")));
  test("backward valid base64 but not zlib raises", () => {
    assert.throws(() => op.backward(Buffer.from("not-zlib-data").toString("base64")));
  });
  test("backward_code is executable", () => {
    const encoded = op.forward("test-recovery");
    assert.equal(rc.compile_backward(op.backward_code)(encoded), "test-recovery");
  });
  test("forward int raises", () => assert.throws(() => op.forward(123), E.TypeError));
});

describe("TestTransformationSequence", () => {
  test("empty identity", () => {
    const seq = new TransformationSequence();
    const payload = { a: 1, b: [2, 3] };
    const [out, inv] = seq.forward(payload);
    assert.equal(out, payload);
    assert.deepEqual(inv, []);
  });
  test("single base64", () => {
    const seq = new TransformationSequence({ transformations: [new Base64()] });
    const raw = B("pipeline");
    const [out, inv] = seq.forward(raw);
    assert.equal(out, new Base64().forward(raw));
    assert.equal(inv.length, 1);
    assert.ok(inv[0].includes("backward"));
  });
  test("append single", () => {
    const seq = new TransformationSequence();
    const b64 = new Base64();
    seq.append(b64);
    assert.deepEqual(seq.transformations, [b64]);
  });
  test("append list", () => {
    const seq = new TransformationSequence();
    seq.append([new Base64(), new Zlib()]);
    assert.equal(seq.transformations.length, 2);
  });
  test("repr empty", () => assert.equal(repr(new TransformationSequence()), "TransformationSequence(identity)"));
  test("repr single", () => assert.equal(repr(new TransformationSequence({ transformations: [new Base64()] })), "TransformationSequence(base64)"));
  test("repr chain", () => {
    assert.equal(repr(new TransformationSequence({ transformations: [new Base64(), new Base64()] })), "TransformationSequence(base64 -> base64)");
  });
  test("append none raises", () => assert.throws(() => new TransformationSequence().append(null), E.TypeError));
  test("append string raises", () => assert.throws(() => new TransformationSequence().append("not-a-transformation"), E.TypeError));
  test("append tuple raises", () => assert.throws(() => new TransformationSequence().append(PyTuple.from_iterable([new Base64()])), E.TypeError));
  test("append empty list no change", () => {
    const seq = new TransformationSequence();
    seq.append([]);
    assert.equal(seq.transformations.length, 0);
  });
  test("append mixed list raises", () => assert.throws(() => new TransformationSequence().append([new Base64(), "bad"]), E.TypeError));
  test("append returns self", () => {
    const seq = new TransformationSequence();
    assert.equal(seq.append(new Base64()), seq);
  });
  test("iter", () => {
    const ops = [new Base64(), new Zlib()];
    const seq = new TransformationSequence({ transformations: ops });
    assert.deepEqual([...seq], ops);
  });
  test("forward none with empty seq", () => assert.equal(new TransformationSequence().forward(null)[0], null));
  test("forward none with base64 raises", () => {
    assert.throws(() => new TransformationSequence({ transformations: [new Base64()] }).forward(null), E.TypeError);
  });
  test("inverse codes reversed", () => {
    const seq = new TransformationSequence({ transformations: [new Base64(), new Zlib()] });
    const [, inv] = seq.forward(B("test"));
    assert.equal(inv.length, 2);
    assert.ok(inv[0].toLowerCase().includes("zlib"));
    assert.ok(inv[1].toLowerCase().includes("base64"));
  });
  test("chained round trip base64 then zlib", () => {
    const data = B("round-trip-chain");
    const b64 = new Base64();
    const zl = new Zlib();
    const [out] = new TransformationSequence({ transformations: [b64, zl] }).forward(data);
    assert.ok(b64.backward(zl.backward(out)).equals(data));
  });
});

describe("TestFernetEncryption", () => {
  let key, op, previous;
  const setUp = () => {
    key = Fernet.generate_key();
    previous = LAILA_STUB.encryption_key;
    LAILA_STUB.encryption_key = key;
    op = new FernetEncryption({ key });
  };
  const tearDown = () => {
    LAILA_STUB.encryption_key = previous;
  };
  const t = (name, fn) =>
    test(name, () => {
      setUp();
      try {
        fn();
      } finally {
        tearDown();
      }
    });

  t("round trip", () => {
    const token = op.forward("secret message");
    assert.equal(op.backward(token), "secret message");
  });
  t("wrong key fails", () => {
    const other = new FernetEncryption({ key: Fernet.generate_key() });
    const token = op.forward("payload");
    assert.throws(() => other.backward(token), E.ValueError);
  });
  t("forward empty string", () => assert.equal(op.backward(op.forward("")), ""));
  t("forward non string raises", () => assert.throws(() => op.forward(B("bytes")), E.TypeError));
  t("forward none raises", () => assert.throws(() => op.forward(null), E.TypeError));
  t("backward non string raises", () => assert.throws(() => op.backward(42), E.TypeError));
  t("backward garbage token raises", () => assert.throws(() => op.backward("definitely-not-a-fernet-token"), E.ValueError));
  t("key as string", () => {
    const key_str = Fernet.generate_key().toString("utf8");
    const o = new FernetEncryption({ key: key_str });
    assert.equal(o.backward(o.forward("test")), "test");
  });
  t("round trip unicode", () => {
    const text = "\u2603 snowman \u2764 heart";
    assert.equal(op.backward(op.forward(text)), text);
  });
  t("different encryptions produce different tokens", () => assert.notEqual(op.forward("same"), op.forward("same")));
  t("backward_code is executable", () => {
    const token = op.forward("recovery-test");
    assert.equal(rc.compile_backward(op.backward_code)(token), "recovery-test");
  });
  t("backward_code with mismatched process key raises", () => {
    const token = op.forward("x");
    LAILA_STUB.encryption_key = Fernet.generate_key();
    assert.throws(() => rc.compile_backward(op.backward_code)(token), E.ValueError);
  });
  t("key=null resolves the process key", () => {
    const o = new FernetEncryption();
    assert.equal(o.backward(o.forward("via-args")), "via-args");
  });
});

describe("EntryState", () => {
  test("members and lookup", () => {
    assert.deepEqual(
      [...EntryState].map((s) => s.name),
      ["READY", "POOLED", "POOLING", "STAGED", "STALE", "NA"],
    );
    assert.equal(EntryState["READY"], EntryState.READY);
    assert.equal(String(EntryState.READY), "EntryState.READY");
    assert.equal(EntryState.READY.value, 1);
    assert.equal(EntryState.READY.name, "READY");
    assert.throws(() => EntryState.__getitem__("NOPE"), E.KeyError);
  });
  test("EntryNotBuiltError is a RuntimeError", () => {
    assert.ok(new EntryNotBuiltError("x") instanceof E.RuntimeError);
  });
});
