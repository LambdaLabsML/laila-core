/**
 * Shared helpers for dtype-travel round-trip tests across pool backends
 * (port of ``tests/functional/pools/base/compdata_roundtrip.py``).
 *
 * Provides:
 *   - NUMPY_DTYPES / build_np(dtype, shape): payload generators
 *   - envelope_encode(cd) / envelope_decode(envelope): wrap / unwrap a
 *     serialized ComputationalData as a JSON-encodable dict carrying the
 *     transformed blob and its full recovery sequence
 *   - register_dtype_matrix(make_pool, {store_as_json_string}): registers a
 *     dense grid of ``node:test`` cases (14 numpy dtypes x 2 modes) plus the
 *     edge cases the JS ``NDArray`` supports, inside the calling ``describe``.
 *
 * The torch half of the Python matrix has no JS counterpart; structured /
 * subarray / datetime64 dtypes are not representable by ``NDArray`` and are
 * skipped explicitly.
 */
import { test } from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

const S = new URL("../../../src/", import.meta.url).href;
const { NDArray, Complex } = await import(S + "_compat/ndarray.js");
const json = await import(S + "_compat/pyjson.js");
const ENT = await import(S + "entry/index.js");
const { ComputationalData, SimpleConstitution, TransformationSequence, Base64, Zlib, transformation_base64 } = ENT;

// =============================================================================
// Payload generators
// =============================================================================

export const NUMPY_DTYPES = ["int8", "int16", "int32", "int64", "uint8", "uint16", "uint32", "uint64", "float16", "float32", "float64", "bool_", "complex64", "complex128"];

const _DT = {
  int8: "|i1",
  int16: "<i2",
  int32: "<i4",
  int64: "<i8",
  uint8: "|u1",
  uint16: "<u2",
  uint32: "<u4",
  uint64: "<u8",
  float16: "<f2",
  float32: "<f4",
  float64: "<f8",
  bool_: "|b1",
  complex64: "<c8",
  complex128: "<c16",
};

export function linspace(a, b, num) {
  if (num === 0) return [];
  if (num === 1) return [a];
  return Array.from({ length: num }, (_, i) => a + ((b - a) * i) / (num - 1));
}

/** Build a deterministic array covering positive + negative values. */
export function build_np(dtype_name, shape = [2, 3]) {
  const dt = _DT[dtype_name] ?? dtype_name;
  const size = shape.length ? shape.reduce((a, b) => a * b, 1) : 1;
  const kind = dt[1];
  let flat;
  if (kind === "b") flat = Array.from({ length: size }, (_, i) => i % 2 === 0);
  else if (kind === "u") flat = Array.from({ length: size }, (_, i) => i);
  else if (kind === "i") flat = Array.from({ length: size }, (_, i) => i - Math.floor(size / 2));
  else if (kind === "f") flat = linspace(-1.0, 1.0, size);
  else if (kind === "c") {
    const real = linspace(-1.0, 1.0, size);
    const imag = linspace(0.5, -0.5, size);
    flat = real.map((r, i) => new Complex(r, imag[i]));
  } else flat = new Array(size).fill(0);
  if (!shape.length) return new NDArray({ dtype: dt, shape: [], data: [size ? flat[0] : 0] });
  return new NDArray({ dtype: dt, shape, data: flat });
}

// =============================================================================
// Envelope helpers
// =============================================================================

/**
 * Serialize *cd* and wrap it in a JSON-encodable dict:
 * ``{blob: <str>, recovery: [<inverse codes...>, <serializer_backward>]}``.
 */
export function envelope_encode(cd, chain = null) {
  const [payload, ser_back] = cd.serialize();
  const ts = chain !== null ? chain : transformation_base64;
  const [transformed, inverse_codes] = ts.forward(payload);
  return { blob: transformed, recovery: [...inverse_codes, ser_back] };
}

/** Reverse ``envelope_encode``; returns the recovered raw payload (not the CD). */
export function envelope_decode(envelope) {
  const recovered = new SimpleConstitution({ codes: envelope.recovery }).build(envelope.blob);
  return recovered instanceof ComputationalData ? recovered.data : recovered;
}

// =============================================================================
// Pool round-trip helper
// =============================================================================

export function pool_roundtrip(pool, key, cd, { store_as_json_string = true, chain = null } = {}) {
  const env = envelope_encode(cd, chain);
  pool[key] = store_as_json_string ? json.dumps(env) : env;
  let got = pool[key];
  if (typeof got === "string") got = json.loads(got);
  return envelope_decode(got);
}

// =============================================================================
// Assertion helpers
// =============================================================================

export function assert_np_roundtrip(recovered, original) {
  assert.ok(recovered instanceof NDArray, `expected NDArray, got ${recovered?.constructor?.name}`);
  assert.equal(recovered.dtype, original.dtype, `numpy dtype drift: ${original.dtype} -> ${recovered.dtype}`);
  assert.deepEqual([...recovered.shape], [...original.shape]);
  assert.ok(recovered.array_equal(original, { equal_nan: true }), `${recovered} != ${original}`);
}

// =============================================================================
// Test registration
// =============================================================================

/**
 * Run a body on a fresh macrotask: backends with a synchronous bridge over
 * an async driver (HDF5 / DuckDB) block on the loop pump, which is impossible
 * from the microtask ``node:test`` invokes test bodies on.
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

function _safe_close(pool) {
  if (typeof pool?.close !== "function") return;
  try {
    pool.close();
  } catch {
    // ignore
  }
}

/**
 * Register the dtype matrix + edge cases as ``node:test`` cases in the
 * enclosing ``describe``. ``make_pool`` is a zero-arg factory producing a
 * fresh pool; the harness closes it after each case.
 * @returns {number} number of registered cases
 */
export function register_dtype_matrix(make_pool, { store_as_json_string = true } = {}) {
  let registered = 0;

  const _roundtrip = (pool, key, cd, mode) => {
    const chain = mode === "raw_b64" ? null : new TransformationSequence({ transformations: [new Base64(), new Zlib()] });
    return pool_roundtrip(pool, key, cd, { store_as_json_string, chain });
  };

  // --- numpy dtype matrix ---
  for (const dtype_name of NUMPY_DTYPES) {
    for (const mode of ["raw_b64", "chain_b64_zlib"]) {
      test(`pool np ${dtype_name} ${mode}`, () =>
        macrotask(() => {
          const arr = build_np(dtype_name, [2, 3]);
          const pool = make_pool();
          try {
            const cd = new ComputationalData(arr);
            assert_np_roundtrip(_roundtrip(pool, `np_${dtype_name}_${mode}`, cd, mode), arr);
          } finally {
            _safe_close(pool);
          }
        }));
      registered += 1;
    }
  }

  // --- numpy edges ---
  const _np_edge = (name, build) => {
    test(`pool np edge ${name}`, () =>
      macrotask(() => {
        const arr = build();
        const pool = make_pool();
        try {
          const cd = new ComputationalData(arr);
          assert_np_roundtrip(_roundtrip(pool, `np_edge_${name}`, cd, "raw_b64"), arr);
        } finally {
          _safe_close(pool);
        }
      }));
    registered += 1;
  };

  _np_edge("fortran_order", () => new NDArray({ dtype: "<f8", shape: [3, 4], data: Array.from({ length: 12 }, (_, i) => i), fortran_order: true }));
  _np_edge("big_endian_i4", () => NDArray.arange(8, ">i4"));
  _np_edge("little_endian_i4", () => NDArray.arange(8, "<i4"));
  _np_edge("big_endian_f8", () => new NDArray({ dtype: ">f8", shape: [6], data: linspace(-1.0, 1.0, 6) }));
  _np_edge("nan_inf_float32", () => new NDArray({ dtype: "<f4", shape: [5], data: [NaN, Infinity, -Infinity, 0.0, 1.0] }));
  _np_edge("subnormal_float64", () => new NDArray({ dtype: "<f8", shape: [3], data: [2.2250738585072014e-308 / 2, 2.2250738585072014e-308, 0.0] }));
  _np_edge("min_max_int64", () => new NDArray({ dtype: "<i8", shape: [3], data: [-(2n ** 63n), 0n, 2n ** 63n - 1n] }));
  _np_edge("zero_d_scalar", () => new NDArray({ dtype: "<f4", shape: [], data: [3.5] }));
  _np_edge("empty_1d", () => new NDArray({ dtype: "<i4", shape: [0], data: [] }));
  _np_edge("complex_with_imag", () => new NDArray({ dtype: "<c8", shape: [3], data: [new Complex(1, 2), new Complex(-3, 4), new Complex(0, -1)] }));
  _np_edge("three_dim", () => NDArray.arange(24, "<i2").reshape([2, 3, 4]));
  // structured_dtype / subarray_dtype / datetime64 / non-contiguous views:
  // not representable by the JS NDArray (always contiguous, scalar dtypes).

  return registered;
}

// =============================================================================
// Small temp-dir factory (shared)
// =============================================================================

export function make_tmpdir(prefix) {
  return fs.mkdtempSync(path.join(os.tmpdir(), prefix));
}
