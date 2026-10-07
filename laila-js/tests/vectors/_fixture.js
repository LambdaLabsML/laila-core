/** Shared loader for ``codecs.json`` plus the JS-side constructions of every case. */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import nodezlib from "node:zlib";
import { PyTuple, PyFloat, PyByteArray, PyFrozenSet } from "../../src/_compat/pytypes.js";
import { NDArray, Complex } from "../../src/_compat/ndarray.js";

const HERE = path.dirname(fileURLToPath(import.meta.url));
export const FIXTURE = JSON.parse(fs.readFileSync(path.join(HERE, "codecs.json"), "utf8"));
export const LAILA_C_VECTORS = path.resolve(HERE, "../../../laila-c/tests/vectors");

/** Decode a fixture payload (``hex`` or ``z:<base64(zlib)>``). */
export function decode(s) {
  if (s.startsWith("z:")) return nodezlib.inflateSync(Buffer.from(s.slice(2), "base64"));
  return Buffer.from(s, "hex");
}

const T = (...xs) => PyTuple.from_iterable(xs);
/** ``np.linspace(a, b, n)``: ``a + step * i`` with ``step = (b - a) / (n - 1)``. */
const linspace = (a, b, n) => {
  const step = (b - a) / (n - 1);
  return Array.from({ length: n }, (_, i) => a + step * i);
};
const F = (x) => new PyFloat(x);
const B = (...xs) => Buffer.from(xs);
const rng = (n) => Array.from({ length: n }, (_, i) => i);
const kdict = (n) => Object.fromEntries(rng(n).map((i) => [`k${i}`, i]));
const shared = [1, 2];

/** JS values mirroring the Python objects in ``gen_vectors.py`` (same names). */
export const CASES = {
  none: null,
  true: true,
  false: false,
  "float_3.5": 3.5,
  float_neg: -2.5e-10,
  "float_1.0": F(1),
  "float_0.0": F(0),
  float_inf: Infinity,
  float_nan: NaN,
  str_empty: "",
  str_hello: "hello laila",
  str_unicode: "\u00fcn\u00efc\u00f6d\u00e9 \u{1f600}",
  str_255: "z".repeat(255),
  str_256: "z".repeat(256),
  str_300: "x".repeat(300),
  str_70000: "y".repeat(70000),
  str_surrogate: "a\udcffb",
  bytes_empty: B(),
  bytes_small: B(0, 1, 2, 0xfe, 0xff),
  bytes_255: Buffer.alloc(255, 0x71),
  bytes_256: Buffer.alloc(256, 0x71),
  bytes_300: Buffer.concat([Buffer.from(rng(256)), Buffer.alloc(44)]),
  bytes_65535: Buffer.alloc(65535, 2),
  bytes_65536: Buffer.alloc(65536, 1),
  bytes_70000: Buffer.alloc(70000, 0x7f),
  bytearray: new PyByteArray([1, 2, 3]),
  list_empty: [],
  list_one: [7],
  list_1234: [1, 2, 3, 4],
  list_1000: rng(1000),
  list_1001: rng(1001),
  list_1500: rng(1500),
  list_2000: rng(2000),
  list_2001: rng(2001),
  list_nested: [[1, [2, [3]]], { k: [null, true] }],
  list_floats: [1.5, F(2), -0, 1e300],
  list_same_str: ["shared", "shared"],
  list_shared_list: [shared, shared],
  bool_in_list: [true, false, null, 0, 1],
  neg_fixint: [-1, -32, -33, -127, -128],
  uint_edges: [127, 128, 255, 256, 65535, 65536, 4294967295, 4294967296],
  tuple_empty: T(),
  tuple_1: T(1),
  tuple_2: T(1, 2),
  tuple_3: T(1, 2, 3),
  tuple_4: T(1, 2, 3, 4),
  tuple_nested: T(T(1, 2), T(3, T(4, 5))),
  dict_empty: {},
  dict_one: { k: "v" },
  dict_ordered: { b: 1, a: 2, z: 3 },
  dict_intkeys: new Map([
    [1, "x"],
    [2, "y"],
  ]),
  dict_tuplekey: new Map([[T(1, 2), "y"]]),
  dict_1000: kdict(1000),
  dict_1001: kdict(1001),
  dict_1500: kdict(1500),
  dict_2000: kdict(2000),
  dict_nested: { a: { b: [1, 2, { c: 3 }] } },
  dict_mixed: { i: 1, f: 2.5, s: "x", b: true, n: null, l: [1, "two"] },
  dict_same_str: { a: "a" },
  dict_bytes: { k: B(0xde, 0xad, 0xbe, 0xef) },
  deep: { a: [{ b: T({ c: [1, T(2, 3)] }) }] },
  set_empty: new Set(),
  set_123: new Set([1, 2, 3]),
  set_1000: new Set(rng(1000)),
  set_1001: new Set(rng(1001)),
  set_1500: new Set(rng(1500)),
  frozenset_empty: new PyFrozenSet(),
  frozenset_123: new PyFrozenSet([1, 2, 3]),
  "int_9223372036854775807": 2n ** 63n - 1n,
  "int_-9223372036854775808": -(2n ** 63n),
  "int_18446744073709551615": 2n ** 64n - 1n,
  "int_18446744073709551616": 2n ** 64n,
  "int_1000000000000000000000000000000": 10n ** 30n,
  "int_-1000000000000000000000000000000": -(10n ** 30n),
};
for (const v of [0, 1, 255, 256, 65535, 65536, 2 ** 31 - 1, -1, -128, -129, -32768, -32769, -(2 ** 31), -(2 ** 31) - 1, 2 ** 31, 2 ** 32 - 1, 2 ** 32, 2 ** 53]) CASES[`int_${v}`] = v;

export const NDARRAYS = {
  npy_f64: new NDArray({ dtype: "<f8", shape: [2, 2], data: [1.5, 2.5, 3.5, 4.5] }),
  npy_i32: new NDArray({ dtype: "<i4", shape: [5], data: [1, 2, 3, 4, 5] }),
  npy_u8: NDArray.arange(6, "|u1").reshape([2, 3]),
  npy_bool: NDArray.array([true, false, true]),
  npy_i64: NDArray.arange(10),
  npy_f32_3d: NDArray.zeros([2, 3, 4], "<f4"),
  npy_scalar: new NDArray({ dtype: "<f8", shape: [], data: [7.5] }),
  npy_empty: new NDArray({ dtype: "<f8", shape: [0], data: [] }),
  npy_fortran: new NDArray({ dtype: "<i4", shape: [2, 3], data: [1, 4, 2, 5, 3, 6], fortran_order: true }),
  npy_big_shape: NDArray.zeros(new Array(30).fill(1), "|u1"),
  npy_f16: new NDArray({ dtype: "<f2", shape: [11], data: [1.0, -1.0, 0.1, 65504.0, 6e-8, NaN, Infinity, -Infinity, 0.0, -0.0, 1 / 3] }),
  npy_f16_lin: new NDArray({ dtype: "<f2", shape: [2, 3, 4], data: linspace(-1, 1, 24) }),
  npy_c8: new NDArray({ dtype: "<c8", shape: [3], data: [new Complex(1, 2), new Complex(-3, 4), new Complex(0, -1)] }),
  npy_c16: new NDArray({ dtype: "<c16", shape: [2, 3], data: linspace(-1, 1, 6).map((re, i) => new Complex(re, linspace(0.5, -0.5, 6)[i])) }),
  npy_be_i4: NDArray.arange(5).astype(">i4"),
  npy_be_f8: new NDArray({ dtype: ">f8", shape: [6], data: linspace(-1, 1, 6) }),
  npy_be_f8_fortran: new NDArray({ dtype: ">f8", shape: [2, 3], data: [0, 3, 1, 4, 2, 5], fortran_order: true }),
  npy_u8_max: new NDArray({ dtype: "<u8", shape: [3], data: [0n, 2n ** 63n, 2n ** 64n - 1n] }),
  npy_i8_minmax: new NDArray({ dtype: "<i8", shape: [3], data: [-(2n ** 63n), 0n, 2n ** 63n - 1n] }),
};

export function first_diff(a, b) {
  let i = 0;
  while (i < a.length && i < b.length && a[i] === b[i]) i++;
  return `len ${a.length} vs ${b.length}, first diff @${i}: got ${a.subarray(Math.max(0, i - 8), i + 16).toString("hex")} want ${b.subarray(Math.max(0, i - 8), i + 16).toString("hex")}`;
}
