/**
 * Byte-identity of the JS codecs against CPython / msgpack-python / numpy /
 * cryptography reference output (``codecs.json``, regenerated with
 * ``npm run vectors``).
 */
import { test, describe } from "node:test";
import assert from "node:assert/strict";
import crypto from "node:crypto";
import * as pickle from "../../src/_codecs/pickle.js";
import * as msgpack from "../../src/_codecs/msgpack.js";
import * as npy from "../../src/_codecs/npy.js";
import * as fernet from "../../src/_codecs/fernet.js";
import * as rc from "../../src/_codecs/recovery_codes.js";
import * as b64 from "../../src/_codecs/base64.js";
import * as zlib from "../../src/_codecs/zlib.js";
import { literal_eval } from "../../src/_compat/pyliteral.js";
import { PyTuple, PyFloat, eq } from "../../src/_compat/pytypes.js";
import { repr } from "../../src/_compat/pyrepr.js";
import { FIXTURE, CASES, NDARRAYS, decode, first_diff } from "./_fixture.js";
import { NDArray } from "../../src/_compat/ndarray.js";

const F = (x) => new PyFloat(x);
const T = (...xs) => PyTuple.from_iterable(xs);

describe("pickle protocol 5", () => {
  for (const [name, r] of Object.entries(FIXTURE.cases)) {
    if (!("pickle" in r) || "npy" in r) continue; // ndarray pickles are checked in the npy suite
    test(name, () => {
      assert.ok(name in CASES, `missing JS construction for ${name}`);
      const want = decode(r.pickle);
      const got = pickle.dumps(CASES[name]);
      assert.ok(got.equals(want), first_diff(got, want));
      const back = pickle.loads(want);
      if (name === "float_nan") assert.ok(Number.isNaN(back));
      else assert.ok(eq(back, CASES[name]), `roundtrip: ${repr(back).slice(0, 120)}`);
    });
  }
});

describe("msgpack (use_bin_type=True / raw=False, strict_map_key=False)", () => {
  const norm = (x) => (x instanceof PyTuple ? [...x].map(norm) : Array.isArray(x) ? x.map(norm) : x);
  for (const [name, r] of Object.entries(FIXTURE.cases)) {
    if ("msgpack_error" in r) {
      test(`${name} raises ${r.msgpack_error}`, () => {
        assert.throws(() => msgpack.packb(CASES[name]), (e) => e.constructor.name === r.msgpack_error);
      });
      continue;
    }
    if (!("msgpack" in r)) continue;
    test(name, () => {
      const want = decode(r.msgpack);
      const got = msgpack.packb(CASES[name], { use_bin_type: true });
      assert.ok(got.equals(want), first_diff(got, want));
      const back = msgpack.unpackb(want, { raw: false, strict_map_key: false });
      if (name === "float_nan") assert.ok(Number.isNaN(back));
      else assert.ok(eq(back, norm(CASES[name])), `roundtrip: ${repr(back).slice(0, 120)}`);
    });
  }
  test("ExtType", () => {
    const p = msgpack.packb([new msgpack.ExtType(5, Buffer.from([1, 2, 3])), new msgpack.ExtType(-1, Buffer.alloc(16, 7))]);
    const back = msgpack.unpackb(p);
    assert.equal(back[0].code, 5);
    assert.ok(back[0].data.equals(Buffer.from([1, 2, 3])));
    assert.equal(back[1].code, -1);
    assert.equal(back[1].data.length, 16);
  });
  test("default hook / ExtraData / strict_map_key / streaming", () => {
    assert.ok(msgpack.packb(new Set([1, 2]), { default: (o) => [...o] }).equals(msgpack.packb([1, 2])));
    assert.throws(() => msgpack.unpackb(Buffer.from([0x01, 0x02])), msgpack.ExtraData);
    assert.throws(() => msgpack.unpackb(msgpack.packb(new Map([[1, 2]])), { strict_map_key: true }), /not allowed for map key/);
    const u = new msgpack.Unpacker();
    u.feed(msgpack.packb("a"));
    u.feed(Buffer.concat([msgpack.packb([1, 2]), Buffer.from([0x92, 0x01])]));
    assert.ok(eq([...u], ["a", [1, 2]]));
  });
});

describe("npy (np.save / np.load, allow_pickle=False)", () => {
  for (const [name, r] of Object.entries(FIXTURE.cases)) {
    if (!("npy" in r)) continue;
    test(name, () => {
      const want = decode(r.npy);
      const got = npy.save(NDARRAYS[name]);
      assert.ok(got.equals(want), `${first_diff(got, want)}\n got header ${JSON.stringify(got.subarray(0, 128).toString("latin1"))}`);
      const back = npy.load(want);
      assert.equal(back.dtype, NDARRAYS[name].dtype);
      assert.deepEqual([...back.shape], [...NDARRAYS[name].shape]);
      assert.ok(back.array_equal(NDARRAYS[name], { equal_nan: true }));
      assert.equal(back.tobytes().toString("hex"), NDARRAYS[name].tobytes().toString("hex"));
      // ndarray.__reduce_ex__: protocol 5 (_frombuffer) and protocol 4 (_reconstruct + BUILD)
      for (const [key, proto] of [
        ["pickle", 5],
        ["pickle4", 4],
      ]) {
        if (!(key in r)) continue;
        const want_p = decode(r[key]);
        const got_p = pickle.dumps(NDARRAYS[name], { protocol: proto });
        assert.ok(got_p.equals(want_p), `${key}: ${first_diff(got_p, want_p)}`);
        const back_p = pickle.loads(want_p);
        assert.ok(back_p instanceof NDArray);
        assert.equal(back_p.dtype, NDARRAYS[name].dtype);
        assert.equal(Boolean(back_p.fortran_order), Boolean(NDARRAYS[name].fortran_order));
        assert.ok(back_p.array_equal(NDARRAYS[name], { equal_nan: true }));
      }
    });
  }
  test("fortran tolist is C-ordered", () => {
    assert.ok(eq(NDARRAYS.npy_fortran.tolist(), [[1, 2, 3], [4, 5, 6]]));
  });
  test("rejects bad magic", () => {
    assert.throws(() => npy.load(Buffer.from("not an npy file at all")), /magic string is not correct/);
  });
});

describe("base64 / zlib", () => {
  const z = FIXTURE.zlib;
  const zin = Buffer.from(z.input, "utf8");
  test("b64encode altchars + std", () => {
    assert.equal(b64.b64encode(Buffer.from(Array.from({ length: 256 }, (_, i) => i)), { altchars: Buffer.from("-_") }).toString(), FIXTURE.base64.alt_0_255);
    assert.equal(b64.b64encode(Buffer.from(Array.from({ length: 256 }, (_, i) => i))).toString(), FIXTURE.base64.std_0_255);
  });
  test("b64decode semantics (binascii)", () => {
    assert.equal(b64.b64decode("aGV s\nbG8=").toString(), "hello");
    assert.throws(() => b64.b64decode("aGV s", { validate: true }), b64.Error);
    assert.throws(() => b64.b64decode("aGVsbG8"), /Incorrect padding/);
    assert.throws(() => b64.b64decode("aGVsb"), /cannot be 1 more than a multiple of 4/);
    assert.throws(() => b64.b64decode("\u00e9"), /only ASCII/);
  });
  test("zlib.compress levels are byte-identical", () => {
    assert.equal(b64.b64encode(zlib.compress(zin)).toString(), z.default);
    assert.equal(b64.b64encode(zlib.compress(zin, { level: 9 })).toString(), z.level9);
    assert.equal(b64.b64encode(zlib.compress(zin, { level: 1 })).toString(), z.level1);
    assert.equal(b64.b64encode(zlib.compress(Buffer.from("abc"), { level: 0 })).toString(), z.level0_abc);
    assert.equal(b64.b64encode(zlib.compress(zin, { wbits: -15 })).toString(), z.raw_wbits);
  });
  test("zlib.decompress", () => {
    assert.ok(zlib.decompress(zlib.compress(zin)).equals(zin));
    assert.ok(zlib.decompress(zlib.compress(zin, { wbits: -15 }), { wbits: -15 }).equals(zin));
    assert.throws(() => zlib.decompress(Buffer.from("nope")), zlib.error);
    assert.throws(() => zlib.compress("str"), (e) => e.constructor.name === "TypeError");
  });
});

describe("fernet", () => {
  const fx = FIXTURE.fernet;
  const f = new fernet.Fernet(fx.key);
  const iv = Buffer.from(fx.iv_hex, "hex");
  for (const [name, [token, plain]] of Object.entries(fx.tokens)) {
    test(`token ${name} is byte-identical`, () => {
      assert.equal(f._encrypt_from_parts(Buffer.from(plain, "utf8"), fx.time, iv).toString(), token);
      assert.equal(f.decrypt(token).toString("utf8"), plain);
      assert.equal(f.extract_timestamp(token), fx.time);
    });
  }
  test("ttl / tamper / bad key", () => {
    const [token] = fx.tokens.hello;
    assert.throws(() => f.decrypt(token, 10), fernet.InvalidToken);
    assert.equal(f.decrypt_at_time(token, 10, fx.time + 5).toString("utf8"), fx.tokens.hello[1]);
    assert.throws(() => f.decrypt_at_time(token, 10, fx.time - 100), fernet.InvalidToken); // clock skew
    const t = Buffer.from(token);
    t[t.length - 5] ^= 1;
    assert.throws(() => f.decrypt(t), fernet.InvalidToken);
    assert.throws(() => new fernet.Fernet("short"), (e) => e.constructor.name === "ValueError");
    assert.equal(f.decrypt(f.encrypt(Buffer.from("x"))).toString(), "x");
    assert.ok(new fernet.Fernet(fernet.Fernet.generate_key()) instanceof fernet.Fernet);
    const mf = new fernet.MultiFernet([new fernet.Fernet(fernet.Fernet.generate_key()), f]);
    assert.equal(mf.decrypt(token).toString("utf8"), fx.tokens.hello[1]);
    const rotated = mf.rotate(token);
    assert.equal(mf.decrypt(rotated).toString("utf8"), fx.tokens.hello[1]);
    assert.throws(() => f.decrypt(rotated), fernet.InvalidToken); // now under the primary key
    assert.throws(() => mf.decrypt(rotated, 10), fernet.InvalidToken); // rotate keeps the original (old) timestamp
  });
  test("rotate preserves timestamp", () => {
    const mf = new fernet.MultiFernet([f]);
    assert.equal(f.extract_timestamp(mf.rotate(fx.tokens.hello[0])), fx.time);
  });
  test("fingerprint", () => {
    assert.equal(crypto.createHash("sha256").update(fx.key).digest("hex").slice(0, 12), fx.fingerprint);
  });
});

describe("recovery codes", () => {
  const r = FIXTURE.recovery_codes;
  const fp = FIXTURE.fernet.fingerprint;
  const emitted = {
    base64: rc.emit_base64({}),
    base64_kw: rc.emit_base64({ altchars: Buffer.from("-_"), validate: true }),
    zlib: rc.emit_zlib({}),
    zlib_kw: rc.emit_zlib({ wbits: 15, bufsize: 16384 }),
    json_string: rc.emit_json_string(),
    msgpack: rc.emit_msgpack({}),
    msgpack_kw: rc.emit_msgpack({ use_list: false }),
    numpy: rc.emit_numpy({}),
    pickle: rc.emit_pickle({}),
    pickle_kw: rc.emit_pickle({ fix_imports: false, encoding: "ASCII" }),
    fernet: rc.emit_fernet(fp, {}),
    fernet_kw: rc.emit_fernet(fp, { ttl: 60 }),
  };
  for (const k of Object.keys(emitted)) {
    test(`emit ${k} is string-identical`, () => {
      assert.ok(k in r, `fixture lacks ${k}`);
      assert.equal(emitted[k], r[k]);
    });
    test(`recognize ${k}`, () => {
      const info = rc.recognize(r[k]);
      assert.ok(info);
      assert.equal(info.name, k.replace(/_kw$/, ""));
    });
  }
  test("recognized kwargs / fingerprint", () => {
    assert.ok(eq(rc.recognize(r.base64_kw).kwargs, { altchars: Buffer.from("-_"), validate: true }));
    assert.ok(eq(rc.recognize(r.zlib_kw).kwargs, { wbits: 15, bufsize: 16384 }));
    const fi = rc.recognize(r.fernet_kw);
    assert.equal(fi.fingerprint, fp);
    assert.ok(eq(fi.kwargs, { ttl: 60 }));
    assert.equal(rc.recognize("def backward(x):\n    return x\n"), null);
  });
  test("compile_backward executes natively", () => {
    const tf = FIXTURE.transform_forward;
    assert.equal(rc.compile_backward(r.zlib)(tf.zlib_default), FIXTURE.zlib.input);
    assert.equal(rc.compile_backward(r.zlib)(tf.zlib_level9), FIXTURE.zlib.input);
    assert.ok(rc.compile_backward(r.base64)("aGVsbG8=").equals(Buffer.from("hello")));
    assert.ok(rc.compile_backward(r.base64_kw)(tf.base64_alt).equals(Buffer.from(Array.from({ length: 256 }, (_, i) => i))));
    assert.ok(eq(rc.compile_backward(r.json_string)('{"a":[1,2.5,null]}'), { a: [1, 2.5, null] }));
    assert.ok(eq(rc.compile_backward(r.msgpack)(decode(FIXTURE.cases.dict_mixed.msgpack)), CASES.dict_mixed));
    assert.ok(eq(rc.compile_backward(r.pickle)(decode(FIXTURE.cases.dict_mixed.pickle)), CASES.dict_mixed));
    assert.ok(rc.compile_backward(r.numpy)(decode(FIXTURE.cases.npy_f64.npy)).__eq__(NDARRAYS.npy_f64));
    assert.throws(() => rc.compile_backward("x = 1\n"), (e) => e.constructor.name === "ValueError");
    assert.throws(() => rc.compile_backward(r.zlib)(123), (e) => e.constructor.name === "TypeError");
  });
});

describe("ast.literal_eval subset", () => {
  test("values", () => {
    assert.ok(
      eq(literal_eval("{'a': 1, 'b': [1, 2.0, -3], 'c': (1,), 'd': None, 'e': True, 'f': b'\\x00-_', 'g': 'it\\'s', 'h': {1: 2}}"), {
        a: 1,
        b: [1, F(2), -3],
        c: T(1),
        d: null,
        e: true,
        f: Buffer.from([0, 0x2d, 0x5f]),
        g: "it's",
        h: new Map([[1, 2]]),
      }),
    );
    assert.equal(literal_eval("12345678901234567890"), 12345678901234567890n);
    assert.ok(eq(literal_eval("(1, 2)"), T(1, 2)));
    assert.equal(literal_eval("(1)"), 1);
    assert.ok(eq(literal_eval("()"), T()));
    assert.ok(eq(literal_eval("{1, 2}"), new Set([1, 2])));
    assert.equal(literal_eval("'a' \"b\""), "ab");
    assert.equal(literal_eval("'''a\nb'''"), "a\nb");
    assert.throws(() => literal_eval("foo()"));
    assert.throws(() => literal_eval("1 +"));
  });
});
