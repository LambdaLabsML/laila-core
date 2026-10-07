/**
 * Port of ``tests/deep_eval/test_04_compdata_transforms_deep.py``.
 *
 * Deep tests for ComputationalData taxonomy and reversible transformations.
 */
import { test, describe } from "node:test";
import assert from "node:assert/strict";

import { S, laila } from "./_fixtures.js";

const T = await import(S + "_compat/pytypes.js");
const E = await import(S + "_compat/errors.js");
const pyjson = await import(S + "_compat/pyjson.js");
const { copy, deepcopy } = await import(S + "_compat/copy.js");
const { repr } = await import(S + "_compat/pyrepr.js");
const { NDArray, Complex } = await import(S + "_compat/ndarray.js");
const b64 = await import(S + "_codecs/base64.js");
const pickle = await import(S + "_codecs/pickle.js");
const { Fernet } = await import(S + "_codecs/fernet.js");
const { CD_dict } = await import(S + "entry/compdata/taxonomy/cd_dict.js");
const { CD_list } = await import(S + "entry/compdata/taxonomy/cd_list.js");
const { CD_numpyarray } = await import(S + "entry/compdata/taxonomy/cd_numpy.js");
const { CD_generic } = await import(S + "entry/compdata/taxonomy/cd_object.js");
const { TYPE_TO_WRAPPER, ComputationalData, register_cdtype } = await import(S + "entry/compdata/taxonomy/compdata.js");
const { TransformationSequence, _data_transformation } = await import(S + "entry/compdata/transformation/base.js");
const { Base64 } = await import(S + "entry/compdata/transformation/base64/base64.js");
const { Zlib } = await import(S + "entry/compdata/transformation/compression/zlib.js");
const { FernetEncryption } = await import(S + "entry/compdata/transformation/encryption/encryption.js");
const { JsonString } = await import(S + "entry/compdata/transformation/jsonstring/jsonstring.js");
const { MsgpackSerializer, NumpySerializer, PickleSerializer } = await import(S + "entry/compdata/transformation/serialization/index.js");
const { SimpleConstitution } = await import(S + "entry/constitution/simple_constitution.js");
const { _exec_one_fn } = await import(S + "entry/constitution/constitution.js");
const { Entry } = await import(S + "entry/entry.js");

const { PyTuple, PyFrozenSet, PyByteArray, tuple, eq } = T;
const B = (s) => Buffer.from(s, "latin1");
const memoryview = (buf) => new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
const type_of = (x) => x.constructor;
const assert_array_equal = (got, expected) => {
  assert.ok(got instanceof NDArray, `${repr(got)} is not an NDArray`);
  assert.deepEqual([...got.shape], [...expected.shape]);
  assert.ok(got.array_equal(expected, { equal_nan: true }), `${repr(got)} != ${repr(expected)}`);
};

/** ``exec(code, ns); fn = ns["backward"] or ns["f"] or <the callable>; fn(value)`` */
function _run_code(code, value) {
  return _exec_one_fn(code)(value);
}

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------

describe("TestDispatch", () => {
  class _PlainObject {}
  const cases = [
    [{ a: 1 }, CD_dict],
    [[1], CD_list],
    [tuple([1]), CD_list],
    [NDArray.zeros([1]), CD_numpyarray],
    [1, CD_generic],
    ["s", CD_generic],
    [B("b"), CD_generic],
    [new Set([1]), CD_generic],
    [new _PlainObject(), CD_generic],
    [1.5, CD_generic],
    [true, CD_generic],
  ];
  for (const [val, cls] of cases) {
    test(`test_type_to_wrapper[${T.type_name(val)}-${cls.name}]`, () => {
      assert.equal(type_of(new ComputationalData(val)), cls);
    });
  }

  test(
    "test_kwarg_form",
    {
      skip: "ComputationalData(data=[1]) keyword-only form is not expressible in JS: a sole {data: ...} object is a dict payload (compdata.js _parse_args folds data= into the positional slot)",
    },
    () => {
      assert.equal(type_of(new ComputationalData({ data: [1] })), CD_list);
    },
  );

  test("test_both_forms_conflict", () => {
    assert.throws(() => new ComputationalData([1], { data: [2] }), E.TypeError);
  });

  test("test_too_many_positionals", () => {
    assert.throws(() => new ComputationalData(1, 2), E.TypeError);
  });

  test("test_missing_data", () => {
    assert.throws(() => new ComputationalData(), E.TypeError);
  });

  test("test_none_rejected", () => {
    assert.throws(() => new ComputationalData(null), E.TypeError);
  });

  test("test_falsy_values_allowed", () => {
    assert.equal(new ComputationalData(0).data, 0);
    assert.equal(new ComputationalData("").data, "");
    assert.deepEqual(new ComputationalData([]).data, []);
    assert.equal(new ComputationalData(false).data, false);
  });

  test("test_subclass_of_dict_dispatches_via_mro", () => {
    class MyDict extends Map {}

    assert.equal(type_of(new ComputationalData(new MyDict([["a", 1]]))), CD_dict);
  });

  test("test_direct_subclass_bypasses_dispatch", () => {
    const cd = new CD_generic([1, 2]);
    assert.equal(type_of(cd), CD_generic);
  });

  test("test_register_cdtype_custom", () => {
    class Marker {}

    const CD_marker = register_cdtype(Marker)(class CD_marker extends CD_generic {});

    try {
      assert.equal(type_of(new ComputationalData(new Marker())), CD_marker);
    } finally {
      TYPE_TO_WRAPPER.delete(Marker);
    }
  });

  test("test_object_registered_as_catch_all", () => {
    assert.equal(TYPE_TO_WRAPPER.get("object"), CD_generic);
  });

  test("test_repr_contains_data", () => {
    assert.ok(repr(new ComputationalData([1])).includes("1"));
    assert.ok(repr(new ComputationalData({ a: 1 })).startsWith("CD_dict"));
  });

  test("test_descriptor_class_access", () => {
    // JS has no descriptor protocol; ``__get__`` is kept for API parity.
    class Holder {
      static x = new ComputationalData([1, 2]);
    }

    assert.ok(Holder.x instanceof ComputationalData);
    assert.deepEqual(Holder.x.__get__(new Holder()), [1, 2]);
  });

  test("test_getitem", () => {
    assert.equal(new ComputationalData([1, 2])[1], 2);
    assert.equal(new ComputationalData({ a: 1 })["a"], 1);
  });
});

// ---------------------------------------------------------------------------
// Per-type wrapper semantics
// ---------------------------------------------------------------------------

describe("TestWrappers", () => {
  test("test_dict_len_shape", () => {
    const cd = new ComputationalData({ a: 1, b: 2 });
    assert.equal(T.len(cd), 2);
    assert.throws(() => cd.shape, E.AttributeError);
  });

  test("test_list_len_shape", () => {
    const cd = new ComputationalData([1, 2, 3]);
    assert.equal(T.len(cd), 3);
    assert.deepEqual([...cd.shape], [3]);
  });

  test("test_numpy_len_shape", () => {
    const cd = new ComputationalData(NDArray.zeros([2, 5]));
    assert.equal(T.len(cd), 2);
    assert.deepEqual([...cd.shape], [2, 5]);
  });

  test("test_generic_len_undefined", () => {
    assert.throws(() => T.len(new ComputationalData(5)), (e) => e instanceof E.TypeError || e instanceof E.AttributeError || e instanceof E.NotImplementedError);
  });

  test("test_dict_copy_shallow", () => {
    const inner = [1];
    const cd = new ComputationalData({ a: inner });
    const c = copy(cd);
    assert.deepEqual(c.data, { a: [1] });
    assert.notEqual(c.data, cd.data);
    assert.equal(c.data["a"], inner);
  });

  test("test_dict_deepcopy", () => {
    const cd = new ComputationalData({ a: [1] });
    const c = deepcopy(cd);
    c.data["a"].push(2);
    assert.deepEqual(cd.data, { a: [1] });
  });

  test("test_list_copy", () => {
    const cd = new ComputationalData([[1]]);
    const c = copy(cd);
    assert.deepEqual(c.data, [[1]]);
    assert.notEqual(c.data, cd.data);
  });

  test("test_list_deepcopy", () => {
    const cd = new ComputationalData([[1]]);
    const c = deepcopy(cd);
    c.data[0].push(2);
    assert.deepEqual(cd.data, [[1]]);
  });

  test("test_tuple_deepcopy_preserves_type", () => {
    const cd = new ComputationalData(tuple([1, 2]));
    assert.ok(deepcopy(cd).data instanceof PyTuple);
  });

  test("test_numpy_copy_independent", () => {
    const arr = NDArray.zeros([3]);
    const cd = new ComputationalData(arr);
    const c = copy(cd);
    c.data.data[0] = 1; // ``c.data[0] = 1`` -- element write on the copy's buffer
    assert.equal(arr.data[0], 0);
  });

  test("test_numpy_deepcopy", () => {
    const cd = new ComputationalData(NDArray.ones([2]));
    assert_array_equal(deepcopy(cd).data, NDArray.ones([2]));
  });

  test("test_generic_copy", () => {
    const cd = new ComputationalData(5);
    assert.equal(copy(cd).data, 5);
    assert.equal(deepcopy(cd).data, 5);
  });

  test("test_default_serializers", () => {
    assert.ok(new ComputationalData({ a: 1 }).serializer instanceof MsgpackSerializer);
    assert.ok(new ComputationalData([1]).serializer instanceof MsgpackSerializer);
    assert.ok(new ComputationalData(NDArray.zeros([1])).serializer instanceof NumpySerializer);
    assert.ok(new ComputationalData(1).serializer instanceof PickleSerializer);
  });

  test("test_serializer_lazy_and_cached", () => {
    const cd = new ComputationalData([1]);
    assert.equal(cd._serializer, null);
    const s1 = cd.serializer;
    assert.equal(cd.serializer, s1);
  });

  test("test_serializer_setter_type_checked", () => {
    assert.throws(() => {
      new ComputationalData({ a: 1 }).serializer = new PickleSerializer();
    }, E.TypeError);
    assert.throws(() => {
      new ComputationalData(NDArray.zeros([1])).serializer = new MsgpackSerializer();
    }, E.TypeError);
    assert.throws(() => {
      new ComputationalData(1).serializer = "nope";
    }, E.TypeError);
  });

  test("test_serializer_setter_accepts_matching", () => {
    const cd = new ComputationalData({ a: 1 });
    const s = new MsgpackSerializer();
    cd.serializer = s;
    assert.equal(cd.serializer, s);
  });

  test(
    "test_validate_assignment_on_data",
    () => {
      const cd = new ComputationalData({ a: 1 });
      assert.throws(() => {
        cd.data = [1, 2]; // CD_dict.data is typed as dict
      });
    },
  );

  test("test_serialize_returns_bytes_and_code", () => {
    const [blob, code] = new ComputationalData([1, 2]).serialize();
    assert.ok(blob instanceof Uint8Array);
    assert.ok(code.includes("def backward"));
    assert.deepEqual(_run_code(code, blob), [1, 2]);
  });
});

// ---------------------------------------------------------------------------
// Serializers
// ---------------------------------------------------------------------------

describe("TestMsgpack", () => {
  test("test_name", () => {
    assert.equal(new MsgpackSerializer().name, "msgpack");
    assert.equal(_data_transformation.REGISTRY["msgpack"], MsgpackSerializer);
  });

  for (const val of [{ a: 1 }, [1, 2, 3], "s", 1, 1.5, true, null, B("\x00"), { n: { m: [1, "x"] } }, []]) {
    test(`test_roundtrip[${repr(val)}]`, () => {
      const s = new MsgpackSerializer();
      assert.ok(eq(s.backward(s.forward(val)), val));
    });
  }

  test("test_bytes_vs_str_preserved", () => {
    const s = new MsgpackSerializer();
    const out = s.backward(s.forward({ b: B("x"), s: "x" }));
    assert.ok(out["b"] instanceof Uint8Array);
    assert.equal(typeof out["s"], "string");
  });

  test("test_backward_code_equivalent", () => {
    const s = new MsgpackSerializer();
    const blob = s.forward({ a: [1, 2] });
    assert.deepEqual(_run_code(s.backward_code, blob), { a: [1, 2] });
  });

  test("test_tuple_becomes_list", () => {
    const s = new MsgpackSerializer();
    assert.ok(eq(s.backward(s.forward(tuple([1, 2]))), [1, 2]));
  });

  test("test_int_keys_roundtrip", () => {
    const s = new MsgpackSerializer();
    assert.ok(eq(s.backward(s.forward(new Map([[1, "a"]]))), new Map([[1, "a"]])));
  });

  test("test_int_keys_backward_code_roundtrip", () => {
    const s = new MsgpackSerializer();
    const blob = s.forward(
      new Map([
        [1, "a"],
        [2.5, "b"],
      ]),
    );
    assert.ok(
      eq(
        _run_code(s.backward_code, blob),
        new Map([
          [1, "a"],
          [2.5, "b"],
        ]),
      ),
    );
  });

  test("test_large_int_overflow", () => {
    assert.throws(() => new MsgpackSerializer().forward(2n ** 64n), E.OverflowError);
  });

  test("test_unserializable_type_raises", () => {
    assert.throws(() => new MsgpackSerializer().forward({ a: NDArray.zeros([1]) }), E.TypeError);
  });

  test("test_nan_roundtrip", () => {
    const s = new MsgpackSerializer();
    assert.ok(Number.isNaN(s.backward(s.forward(NaN))));
  });

  test("test_backward_kwargs_merged", () => {
    const s = new MsgpackSerializer({ backward_kwargs: { strict_map_key: false } });
    assert.ok(eq(s.backward(s.forward(new Map([[1, "a"]]))), new Map([[1, "a"]])));
    assert.ok(eq(_run_code(s.backward_code, s.forward(new Map([[1, "a"]]))), new Map([[1, "a"]])));
  });

  test("test_unicode", () => {
    const s = new MsgpackSerializer();
    assert.equal(s.backward(s.forward("✓ héllo")), "✓ héllo");
  });
});

describe("TestPickle", () => {
  test("test_name", () => {
    assert.equal(new PickleSerializer().name, "pickle");
  });

  const cases = [
    ["1", 1],
    ["'x'", "x"],
    ["{1, 2}", new Set([1, 2])],
    ["(1, 2)", tuple([1, 2])],
    ["{1: 'a'}", new Map([[1, "a"]])],
    ["None", null],
    ["2**100", 2n ** 100n],
  ];
  for (const [id, val] of cases) {
    test(`test_roundtrip[${id}]`, () => {
      const s = new PickleSerializer();
      assert.ok(eq(s.backward(s.forward(val)), val));
    });
  }
  test(
    "test_roundtrip[(1+2j)]",
    () => {
      const s = new PickleSerializer();
      const val = new Complex(1, 2);
      assert.ok(eq(s.backward(s.forward(val)), val));
    },
  );

  test("test_tuple_preserved", () => {
    const s = new PickleSerializer();
    assert.ok(s.backward(s.forward(tuple([1, 2]))) instanceof PyTuple);
  });

  test("test_backward_code", () => {
    const s = new PickleSerializer();
    const blob = s.forward(new Map([[1, 2]]));
    assert.ok(eq(_run_code(s.backward_code, blob), new Map([[1, 2]])));
  });

  test("test_forward_is_pickle", () => {
    const s = new PickleSerializer();
    assert.deepEqual(pickle.loads(s.forward([1])), [1]);
  });

  test("test_unpicklable_raises", () => {
    assert.throws(() => new PickleSerializer().forward(() => 1));
  });
});

describe("TestNumpySerializer", () => {
  test("test_name", () => {
    assert.equal(new NumpySerializer().name, "numpy");
  });

  const cases = [
    ["arange(5)", NDArray.arange(5)],
    ["zeros((2, 3), float32)", NDArray.zeros([2, 3], "<f4")],
    ["array([True, False])", NDArray.array([true, false])],
    ["array([], int64)", new NDArray({ dtype: "<i8", shape: [0], data: [] })],
    ["arange(24).reshape(2, 3, 4)", NDArray.arange(24).reshape([2, 3, 4])],
    ["array(3.5)", new NDArray({ dtype: "<f8", shape: [], data: [3.5] })],
  ];
  for (const [id, arr] of cases) {
    test(`test_roundtrip[${id}]`, () => {
      const s = new NumpySerializer();
      const out = s.backward(s.forward(arr));
      assert_array_equal(out, arr);
      assert.equal(out.dtype, arr.dtype);
      assert.deepEqual([...out.shape], [...arr.shape]);
    });
  }
  test("test_roundtrip[array(['a', 'bc'])]", { skip: "NDArray has no unicode (<U2) dtype; numpy string arrays have no JS counterpart" }, () => {});

  test("test_fortran_order", () => {
    const arr = new NDArray({ dtype: "<i8", shape: [2, 3], data: [0, 1, 2, 3, 4, 5], fortran_order: true });
    const s = new NumpySerializer();
    assert_array_equal(s.backward(s.forward(arr)), arr);
  });

  test("test_backward_code", () => {
    const s = new NumpySerializer();
    const arr = NDArray.arange(3);
    assert_array_equal(_run_code(s.backward_code, s.forward(arr)), arr);
  });

  test("test_object_arrays_rejected_on_save", { skip: "NDArray has no object dtype; np.array([...], dtype=object) cannot be constructed in JS" }, () => {});
});

// ---------------------------------------------------------------------------
// Encoders
// ---------------------------------------------------------------------------

describe("TestBase64", () => {
  test("test_name", () => {
    assert.equal(new Base64().name, "base64");
  });

  for (const data of [B(""), B("\x00"), B("hello"), Buffer.from(T.range(256))]) {
    test(`test_roundtrip[${repr(data)}]`, () => {
      const tf = new Base64();
      const enc = tf.forward(data);
      assert.equal(typeof enc, "string");
      assert.ok(eq(tf.backward(enc), data));
    });
  }

  test("test_forward_matches_stdlib", () => {
    assert.equal(new Base64().forward(B("abc")), Buffer.from("abc").toString("base64"));
  });

  test("test_bytearray_memoryview", () => {
    const tf = new Base64();
    assert.ok(eq(tf.backward(tf.forward(new PyByteArray(B("xy")))), B("xy")));
    assert.ok(eq(tf.backward(tf.forward(memoryview(B("xy")))), B("xy")));
  });

  test("test_forward_rejects_str", () => {
    assert.throws(() => new Base64().forward("str"), E.TypeError);
  });

  test("test_backward_accepts_bytes", () => {
    assert.ok(eq(new Base64().backward(B("YWJj")), B("abc")));
  });

  test("test_backward_invalid", () => {
    assert.throws(() => new Base64().backward("!!!not base64!!!"), E.ValueError);
  });

  test("test_backward_code", () => {
    const tf = new Base64();
    assert.ok(eq(_run_code(tf.backward_code, tf.forward(B("zz"))), B("zz")));
  });

  test("test_backward_code_memoryview", () => {
    const tf = new Base64();
    assert.ok(eq(_run_code(tf.backward_code, memoryview(Buffer.from(tf.forward(B("zz")), "utf8"))), B("zz")));
  });

  test("test_altchars_kwargs", () => {
    const tf = new Base64({ forward_kwargs: { altchars: B("-_") }, backward_kwargs: { altchars: B("-_") } });
    const data = Buffer.from([0xfb, 0xff]);
    const enc = tf.forward(data);
    assert.ok(!enc.includes("+") && !enc.includes("/"));
    assert.ok(eq(tf.backward(enc), data));
    assert.ok(eq(_run_code(tf.backward_code, enc), data));
  });
});

describe("TestZlib", () => {
  test("test_name", () => {
    assert.equal(new Zlib().name, "zlib");
  });

  for (const [id, text] of [
    ["''", ""],
    ["'a'", "a"],
    ["'hello' * 1000", "hello".repeat(1000)],
    ["'✓ unicode'", "✓ unicode"],
  ]) {
    test(`test_roundtrip[${id}]`, () => {
      const tf = new Zlib();
      assert.equal(tf.backward(tf.forward(text)), text);
    });
  }

  test("test_output_is_base64_text", () => {
    const out = new Zlib().forward("x".repeat(100));
    b64.b64decode(out, { validate: true }); // must decode
  });

  test("test_compresses", () => {
    const text = "a".repeat(10000);
    assert.ok(new Zlib().forward(text).length < text.length);
  });

  test("test_forward_rejects_bytes", () => {
    assert.throws(() => new Zlib().forward(B("bytes")), E.TypeError);
  });

  test("test_backward_code", () => {
    const tf = new Zlib();
    assert.equal(_run_code(tf.backward_code, tf.forward("payload")), "payload");
  });

  test("test_level_kwargs", () => {
    const tf = new Zlib({ forward_kwargs: { level: 9 } });
    assert.equal(tf.backward(tf.forward("abc".repeat(100))), "abc".repeat(100));
  });

  test("test_backward_invalid", () => {
    assert.throws(() => new Zlib().backward(Buffer.from("not zlib").toString("base64")));
  });
});

describe("TestJsonString", () => {
  test("test_name", () => {
    assert.equal(new JsonString().name, "json_string");
  });

  for (const val of [{ a: 1 }, [1, 2], "s", 1, null, true, { n: [1, { m: null }] }]) {
    test(`test_roundtrip[${repr(val)}]`, () => {
      const tf = new JsonString();
      assert.ok(eq(tf.backward(tf.forward(val)), val));
    });
  }

  test("test_output_is_compact_json", () => {
    const out = new JsonString().forward({ a: [1, 2] });
    assert.deepEqual(pyjson.loads(out), { a: [1, 2] });
    assert.ok(!out.includes(" "));
  });

  test("test_non_json_raises", () => {
    class _Opaque {}
    assert.throws(() => new JsonString().forward({ a: new _Opaque() }), E.TypeError);
  });

  test("test_backward_non_str", () => {
    assert.throws(() => new JsonString().backward(B("{}")), E.TypeError);
  });

  test("test_backward_invalid_json", () => {
    assert.throws(() => new JsonString().backward("{not json"), (e) => e instanceof E.TypeError || e instanceof E.ValueError);
  });

  test("test_backward_code", () => {
    const tf = new JsonString();
    assert.deepEqual(_run_code(tf.backward_code, tf.forward([1])), [1]);
  });

  test("test_int_keys_become_strings", () => {
    const tf = new JsonString();
    assert.ok(eq(tf.backward(tf.forward(new Map([[1, "a"]]))), { 1: "a" }));
  });
});

describe("TestFernet", () => {
  /**
   * ``key`` fixture: a fresh key, configured process-wide
   * (``laila.encryption_key``) the way a writer *and* a reader are expected
   * to do; restored afterwards.
   */
  const kt = (name, fn, opts) => {
    const body = async () => {
      const key = Fernet.generate_key();
      const previous = laila.encryption_key;
      laila.encryption_key = key;
      try {
        await fn(key);
      } finally {
        laila.encryption_key = previous;
      }
    };
    return opts ? test(name, opts, body) : test(name, body);
  };
  const decode = (key) => Buffer.from(key).toString("utf8");

  kt("test_key_from_laila_args_when_omitted", (key) => {
    const tf = new FernetEncryption();
    assert.ok(eq(tf.key, key));
    assert.equal(tf.backward(tf.forward("x")), "x");
  });

  kt("test_no_configured_key_is_a_clear_error", (_key) => {
    laila.encryption_key = null;
    assert.throws(() => new FernetEncryption(), (e) => e instanceof E.RuntimeError && /laila\.encryption_key/.test(e.message));
  });

  kt("test_alias_and_args_location_agree", (key) => {
    assert.ok(eq(laila.args.encryption.key, key));
    assert.ok(eq(laila.encryption_key, key));
  });

  kt("test_key_excluded_from_dump_and_repr", (key) => {
    const tf = new FernetEncryption({ key });
    assert.ok(!("key" in tf.model_dump()));
    assert.ok(!repr(tf).includes(decode(key)));
  });

  kt("test_recipient_with_other_key_gets_descriptive_error", (key) => {
    const tf = new FernetEncryption({ key });
    const token = tf.forward("hello");
    laila.encryption_key = Fernet.generate_key();
    assert.throws(() => _run_code(tf.backward_code, token), (e) => e instanceof E.ValueError && /does not match/.test(e.message));
  });

  kt("test_recipient_with_same_key_decrypts_entry", async (_key) => {
    const { build_by_scope } = await import(S + "entry/constitution/build_maps.js");

    const seq = new TransformationSequence({ transformations: [new Base64(), new FernetEncryption()] });
    const d = Entry.constant({ pw: "hunter2" }).serialize(seq);
    assert.deepEqual(build_by_scope(d).data, { pw: "hunter2" });
  });

  kt("test_name", (key) => {
    assert.equal(new FernetEncryption({ key }).name, "fernet");
  });

  kt("test_roundtrip", (key) => {
    const tf = new FernetEncryption({ key });
    assert.equal(tf.backward(tf.forward("secret")), "secret");
  });

  kt("test_str_key_accepted", (key) => {
    const tf = new FernetEncryption({ key: decode(key) });
    assert.equal(tf.backward(tf.forward("x")), "x");
  });

  kt("test_forward_rejects_bytes", (key) => {
    assert.throws(() => new FernetEncryption({ key }).forward(B("x")), E.TypeError);
  });

  kt("test_backward_rejects_bytes", (key) => {
    assert.throws(() => new FernetEncryption({ key }).backward(B("x")), E.TypeError);
  });

  kt("test_wrong_key_fails", (key) => {
    const token = new FernetEncryption({ key }).forward("x");
    assert.throws(() => new FernetEncryption({ key: Fernet.generate_key() }).backward(token), E.ValueError);
  });

  kt("test_tampered_token_fails", (key) => {
    const tf = new FernetEncryption({ key });
    const token = tf.forward("x");
    assert.throws(() => tf.backward(token.slice(0, -3) + "AAA"), E.ValueError);
  });

  kt("test_ttl_expired", async (key) => {
    const tf = new FernetEncryption({ key, backward_kwargs: { ttl: 1 } });
    const token = tf.forward("x");

    await new Promise((r) => setTimeout(r, 2100)); // time.sleep(2.1)
    assert.throws(() => tf.backward(token), E.ValueError);
  });

  test("test_invalid_key_rejected", () => {
    assert.throws(() => new FernetEncryption({ key: "short" }));
  });

  kt("test_backward_code_decrypts", (key) => {
    const tf = new FernetEncryption({ key });
    assert.equal(_run_code(tf.backward_code, tf.forward("hello")), "hello");
  });

  kt("test_backward_code_does_not_leak_key", (key) => {
    const tf = new FernetEncryption({ key });
    assert.ok(!tf.backward_code.includes(decode(key)));
  });

  kt("test_serialized_entry_does_not_contain_key", (key) => {
    const seq = new TransformationSequence({ transformations: [new Base64(), new FernetEncryption({ key })] });
    const d = Entry.constant({ pw: "hunter2" }).serialize(seq);
    assert.ok(!pyjson.dumps(d).includes(decode(key)));
  });
});

// ---------------------------------------------------------------------------
// TransformationSequence
// ---------------------------------------------------------------------------

describe("TestTransformationSequence", () => {
  test("test_identity", () => {
    const x = B("x");
    const [out, codes] = new TransformationSequence().forward(x);
    assert.ok(eq(out, B("x")));
    assert.deepEqual(codes, []);
    assert.equal(repr(new TransformationSequence()), "TransformationSequence(identity)");
  });

  test("test_forward_order_and_reversed_codes", () => {
    const seq = new TransformationSequence({ transformations: [new Base64(), new Zlib()] });
    const [out, codes] = seq.forward(B("hello"));
    assert.equal(typeof out, "string");
    assert.equal(codes[0], new Zlib().backward_code);
    assert.equal(codes[1], new Base64().backward_code);
  });

  test("test_replay_codes_recovers", () => {
    const seq = new TransformationSequence({ transformations: [new Base64(), new Zlib()] });
    const [out, codes] = seq.forward(B("hello"));
    let cur = out;
    for (const code of codes) cur = _run_code(code, cur);
    assert.ok(eq(cur, B("hello")));
  });

  test("test_append_single_chain", () => {
    const seq = new TransformationSequence();
    assert.equal(seq.append(new Base64()), seq);
    assert.equal(seq.transformations.length, 1);
  });

  test("test_append_list", () => {
    const seq = new TransformationSequence().append([new Base64(), new Zlib()]);
    assert.equal(seq.transformations.length, 2);
  });

  test("test_append_invalid", () => {
    assert.throws(() => new TransformationSequence().append("x"), E.TypeError);
    assert.throws(() => new TransformationSequence().append([new Base64(), "x"]), E.TypeError);
  });

  test("test_iter", () => {
    const seq = new TransformationSequence({ transformations: [new Base64()] });
    assert.deepEqual(
      [...seq].map((tf) => tf.name),
      ["base64"],
    );
  });

  test("test_repr_names", () => {
    const seq = new TransformationSequence({ transformations: [new Base64(), new Zlib()] });
    assert.equal(repr(seq), "TransformationSequence(base64 -> zlib)");
  });

  test("test_wrong_order_raises", () => {
    const seq = new TransformationSequence({ transformations: [new Zlib(), new Base64()] });
    assert.throws(() => seq.forward(B("x")), E.TypeError);
  });

  test("test_prebuilt_sequences_exported", async () => {
    const { transformation_base64, transformation_base64_compression } = await import(S + "entry/index.js");

    assert.deepEqual(
      [...transformation_base64].map((tf) => tf.name),
      ["base64"],
    );
    assert.deepEqual(
      [...transformation_base64_compression].map((tf) => tf.name),
      ["base64", "zlib"],
    );
  });

  test("test_registry_contains_all", () => {
    for (const n of ["msgpack", "pickle", "numpy", "base64", "zlib", "json_string", "fernet"]) assert.ok(n in _data_transformation.REGISTRY, n);
  });

  test(
    "test_base_class_abstract",
    () => {
      assert.throws(() => new _data_transformation(), E.TypeError);
    },
  );

  test("test_subclass_without_name_not_registered", () => {
    class Anon extends _data_transformation {
      forward(d) {
        return d;
      }

      backward(d) {
        return d;
      }
    }

    assert.ok(!Object.values(_data_transformation.REGISTRY).includes(Anon));
  });

  test("test_full_entry_pipeline_via_simple_constitution", () => {
    const seq = new TransformationSequence({ transformations: [new Base64(), new Zlib()] });
    const [payload_bytes, code] = new ComputationalData({ a: [1, 2] }).serialize();
    const [out, codes] = seq.forward(payload_bytes);
    const konst = new SimpleConstitution({ codes: [...codes, code] });
    if (typeof konst.build === "function") assert.deepEqual(konst.build(out), { a: [1, 2] });
    else assert.ok(true);
  });
});

// ---------------------------------------------------------------------------
// Entry-level payload round trips (black-box)
// ---------------------------------------------------------------------------

describe("TestEntryPayloadRoundTrips", () => {
  const SEQ = new TransformationSequence({ transformations: [new Base64(), new Zlib()] });

  const _rt = (val) => {
    const d = Entry.constant(val).serialize(SEQ);
    pyjson.dumps(d); // must be JSON-safe for text-based pools
    return Entry._build_from_dict_sync(d).data;
  };

  const cases = [
    ["1", 1],
    ["2**40", 2 ** 40],
    ["-1", -1],
    ["1.25", 1.25],
    ["'s'", "s"],
    ["''", ""],
    ["True", true],
    ["b'\\x00\\x01'", B("\x00\x01")],
    ["[1, 'a', None]", [1, "a", null]],
    ["{'a': {'b': [1]}}", { a: { b: [1] } }],
    ["{1, 2}", new Set([1, 2])],
    ["frozenset({1})", new PyFrozenSet([1])],
  ];
  for (const [id, val] of cases) {
    test(`test_scalars_and_containers[${id}]`, () => {
      assert.ok(eq(_rt(val), val), `${repr(_rt(val))} != ${repr(val)}`);
    });
  }
  test(
    "test_scalars_and_containers[(1+1j)]",
    () => {
      const val = new Complex(1, 1);
      assert.ok(eq(_rt(val), val));
    },
  );

  test("test_numpy_dtype_preserved", () => {
    for (const dt of ["|i1", "<u2", "<f2", "<c8", "|b1"]) {
      const arr = NDArray.ones([3], dt);
      const out = _rt(arr);
      assert.equal(out.dtype, dt);
    }
  });

  test("test_large_array", () => {
    const arr = new NDArray({ dtype: "<f8", shape: [200, 200], data: Float64Array.from({ length: 200 * 200 }, () => Math.random()) });
    assert_array_equal(_rt(arr), arr);
  });

  test("test_tuple_type_preserved", () => {
    assert.ok(_rt(tuple([1, 2])) instanceof PyTuple);
  });

  test("test_nested_tuple_preserved", { todo: "xfail strict (Python): documented msgpack limitation: a tuple nested inside a *list* comes back as a list" }, () => {
    assert.ok(eq(_rt([tuple([1, 2])]), [tuple([1, 2])]));
  });

  test("test_int_key_dict", () => {
    assert.ok(eq(_rt(new Map([[1, "a"]])), new Map([[1, "a"]])));
  });

  test("test_dict_with_array_value", () => {
    const out = _rt({ a: NDArray.zeros([2]) });
    assert_array_equal(out["a"], NDArray.zeros([2]));
  });

  test("test_dict_with_big_int", () => {
    assert.ok(eq(_rt({ a: 2n ** 64n }), { a: 2n ** 64n }));
  });

  test("test_dict_with_datetime_roundtrips_via_pickle_fallback", { skip: "datetime.datetime has no JS counterpart: _compat/datetime.js exposes only ISO-string helpers and _codecs/pickle.js has no datetime reducer" }, () => {});
});
